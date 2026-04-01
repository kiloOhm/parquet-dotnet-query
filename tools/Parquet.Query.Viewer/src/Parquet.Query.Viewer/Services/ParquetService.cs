using System.Diagnostics;
using System.IO.Compression;
using System.Text.Json;
using Parquet;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Schema;
using Parquet.Query.Dynamic;
using Parquet.Query.Viewer.Models;

namespace Parquet.Query.Viewer.Services;

public sealed class ParquetService : IDisposable
{
    private ParquetReader? _reader;
    private string? _currentFilePath;
    private ParquetOptions? _currentOptions;
    private EncryptionConfig? _currentEncryption;
    private long[]? _rowGroupRowCounts;
    private long _totalRowCount;
    private Dictionary<string, IndexEntry[]>? _indexEntryCache;

    public bool HasOpenFile => _reader is not null;
    public string? CurrentFilePath => _currentFilePath;

    /// <summary>
    /// Checks the file's magic bytes to detect encrypted footer (PARE vs PAR1).
    /// </summary>
    public static bool DetectEncryptedFooter(string filePath)
    {
        using var fs = System.IO.File.OpenRead(filePath);
        Span<byte> magic = stackalloc byte[4];
        return fs.Read(magic) == 4
            && magic[0] == 'P' && magic[1] == 'A' && magic[2] == 'R' && magic[3] == 'E';
    }

    public async Task<ParquetFileInfo> OpenFileAsync(string filePath, EncryptionConfig? encryption = null)
    {
        _reader?.Dispose();
        _currentFilePath = filePath;
        _currentEncryption = encryption;
        _currentOptions = BuildOptions(encryption);

        _reader = await ParquetReader.CreateAsync(filePath, _currentOptions);

        _rowGroupRowCounts = new long[_reader.RowGroupCount];
        _totalRowCount = 0;
        for (int i = 0; i < _reader.RowGroupCount; i++)
        {
            using var rg = _reader.OpenRowGroupReader(i);
            _rowGroupRowCounts[i] = rg.RowCount;
            _totalRowCount += rg.RowCount;
        }

        return BuildFileInfo(filePath);
    }

    public SchemaInfo GetSchema()
    {
        EnsureOpen();
        var topFields = _reader!.Schema.Fields;
        return new SchemaInfo(topFields.Select(f => new ColumnInfo(
            f.Name,
            f.Name,
            FieldTypeName(f),
            FieldTypeName(f),
            f.IsNullable,
            f is ListField || f is MapField
        )).ToArray());
    }

    private static string FieldTypeName(Field f) => f switch
    {
        DataField df => df.ClrType.Name,
        MapField => "Map",
        ListField lf => $"List<{FieldTypeName(lf.Item)}>",
        StructField => "Struct",
        _ => f.SchemaType.ToString()
    };

    public FileMetadataInfo GetMetadata()
    {
        EnsureOpen();
        var fileInfo = BuildFileInfo(_currentFilePath!);
        var rowGroups = GetRowGroupsMetadata();
        var footer = GetFooterInfo();
        var encryption = GetEncryptionInfo();
        return new FileMetadataInfo(fileInfo, rowGroups, footer, encryption);
    }

    public async Task<DataPage> GetDataAsync(int offset, int limit)
    {
        EnsureOpen();
        var topFields = _reader!.Schema.Fields;
        var columns = topFields.Select(f => f.Name).ToArray();
        var dataTypes = topFields.Select(FieldTypeName).ToArray();
        var rows = new List<object?[]>();

        long currentRow = 0;
        for (int rg = 0; rg < _reader.RowGroupCount && rows.Count < limit; rg++)
        {
            var rgRowCount = _rowGroupRowCounts![rg];

            if (currentRow + rgRowCount <= offset)
            {
                currentRow += rgRowCount;
                continue;
            }

            using var rgReader = _reader.OpenRowGroupReader(rg);

            // Read all leaf DataColumns with their repetition/definition levels
            var allDataFields = _reader.Schema.GetDataFields();
            var dataColumns = new Dictionary<string, DataColumn>();
            foreach (var df in allDataFields)
            {
                if (rgReader.ColumnExists(df))
                {
                    dataColumns[df.Path.ToString()] = await rgReader.ReadColumnAsync(df);
                }
            }

            // Reconstruct per-row values for each top-level field
            var fieldRows = new List<object?[]>();
            for (int f = 0; f < topFields.Count; f++)
            {
                var reconstructed = ReconstructField(topFields[f], dataColumns, (int)rgRowCount);
                // Pad fieldRows if this is the first field
                while (fieldRows.Count < reconstructed.Count)
                    fieldRows.Add(new object?[topFields.Count]);
                for (int r = 0; r < reconstructed.Count; r++)
                    fieldRows[r][f] = reconstructed[r];
            }

            var rgStart = (int)Math.Max(0, offset - currentRow);
            var rgEnd = (int)Math.Min(rgRowCount, offset + limit - currentRow);

            for (int i = rgStart; i < rgEnd && rows.Count < limit; i++)
            {
                if (i < fieldRows.Count)
                    rows.Add(fieldRows[i]);
            }

            currentRow += rgRowCount;
        }

        return new DataPage(columns, dataTypes, rows.ToArray(), offset, limit, _totalRowCount);
    }

    /// <summary>
    /// Reconstructs per-row values for a top-level field from the raw DataColumns.
    /// For primitive fields, returns the values directly.
    /// For Map/List/Struct, groups leaf values using repetition levels.
    /// </summary>
    private List<object?> ReconstructField(Field field, Dictionary<string, DataColumn> dataColumns, int rowCount)
    {
        switch (field)
        {
            case DataField df:
            {
                var path = df.Path.ToString();
                if (!dataColumns.TryGetValue(path, out var dc))
                    return Enumerable.Repeat<object?>(null, rowCount).ToList();

                var result = new List<object?>(rowCount);
                for (int i = 0; i < dc.Data.Length && result.Count < rowCount; i++)
                    result.Add(FormatValue(dc.Data.GetValue(i)));
                // Pad if column is shorter
                while (result.Count < rowCount) result.Add(null);
                return result;
            }

            case MapField mf:
            {
                var keyLeaves = GetLeafDataFields(mf.Key);
                var valLeaves = GetLeafDataFields(mf.Value);
                if (keyLeaves.Count == 0)
                    return Enumerable.Repeat<object?>(null, rowCount).ToList();

                var keyPath = keyLeaves[0].Path.ToString();
                if (!dataColumns.TryGetValue(keyPath, out var keyCol))
                    return Enumerable.Repeat<object?>(null, rowCount).ToList();

                DataColumn? valCol = null;
                if (valLeaves.Count > 0)
                    dataColumns.TryGetValue(valLeaves[0].Path.ToString(), out valCol);

                return GroupByRepetition(
                    keyCol,
                    rowCount,
                    (indices) =>
                    {
                        var dict = new Dictionary<string, object?>();
                        foreach (var idx in indices)
                        {
                            var k = FormatValue(keyCol.Data.GetValue(idx))?.ToString() ?? "";
                            var v = valCol != null && idx < valCol.Data.Length
                                ? FormatValue(valCol.Data.GetValue(idx))
                                : null;
                            dict[k] = v;
                        }
                        return dict.Count > 0 ? dict : null;
                    });
            }

            case ListField lf:
            {
                // Find the leaf data field(s) under this list
                var leafFields = GetLeafDataFields(lf);
                if (leafFields.Count == 0 || !dataColumns.TryGetValue(leafFields[0].Path.ToString(), out var listCol))
                    return Enumerable.Repeat<object?>(null, rowCount).ToList();

                if (leafFields.Count == 1 && lf.Item is DataField)
                {
                    // Simple list of primitives
                    return GroupByRepetition(
                        listCol,
                        rowCount,
                        (indices) =>
                        {
                            var items = indices
                                .Where(i => i < listCol.Data.Length)
                                .Select(i => FormatValue(listCol.Data.GetValue(i))).ToList();
                            return items.Count > 0 ? items : null;
                        });
                }

                // Complex list items (struct inside list) — fall through to flat display
                return ReconstructFlatFallback(leafFields, dataColumns, rowCount);
            }

            case StructField sf:
            {
                var leafFields = GetLeafDataFields(sf);
                if (leafFields.Count == 0)
                    return Enumerable.Repeat<object?>(null, rowCount).ToList();

                var result = new List<object?>(rowCount);
                for (int r = 0; r < rowCount; r++)
                {
                    var obj = new Dictionary<string, object?>();
                    foreach (var lf in leafFields)
                    {
                        if (dataColumns.TryGetValue(lf.Path.ToString(), out var dc) && r < dc.Data.Length)
                            obj[lf.Name] = FormatValue(dc.Data.GetValue(r));
                    }
                    result.Add(obj.Count > 0 ? obj : null);
                }
                return result;
            }

            default:
                return Enumerable.Repeat<object?>(null, rowCount).ToList();
        }
    }

    /// <summary>
    /// Groups a DataColumn's values into per-row collections using repetition levels.
    /// RL=0 marks the start of a new top-level row. Definition levels determine which
    /// RL entries actually have data (the Data array excludes nulls, so it may be shorter
    /// than the RL array).
    /// </summary>
    private static List<object?> GroupByRepetition(DataColumn dc, int rowCount, Func<List<int>, object?> assembler)
    {
        var result = new List<object?>(rowCount);
        var rl = dc.RepetitionLevels;
        var dl = dc.DefinitionLevels;
        var maxDl = dc.Field.MaxDefinitionLevel;

        if (rl == null || rl.Length == 0)
        {
            // No repetition — one value per row
            for (int i = 0; i < rowCount; i++)
            {
                var indices = i < dc.Data.Length ? new List<int> { i } : new List<int>();
                result.Add(assembler(indices));
            }
            return result;
        }

        // Build a mapping from RL index -> Data index.
        // Data array only contains values where definition level == max (i.e. non-null).
        var rlToDataIdx = new int[rl.Length];
        int dataIdx = 0;
        for (int i = 0; i < rl.Length; i++)
        {
            if (dl != null && dl[i] < maxDl)
            {
                rlToDataIdx[i] = -1; // null entry, no data
            }
            else
            {
                rlToDataIdx[i] = dataIdx < dc.Data.Length ? dataIdx : -1;
                dataIdx++;
            }
        }

        var currentIndices = new List<int>();
        for (int i = 0; i < rl.Length; i++)
        {
            if (rl[i] == 0 && currentIndices.Count > 0)
            {
                result.Add(assembler(currentIndices));
                currentIndices = new List<int>();
            }
            var di = rlToDataIdx[i];
            if (di >= 0)
                currentIndices.Add(di);
        }
        if (currentIndices.Count > 0)
            result.Add(assembler(currentIndices));

        // Pad if fewer rows reconstructed (empty maps/lists at end)
        while (result.Count < rowCount) result.Add(null);
        return result;
    }

    private static List<DataField> GetLeafDataFields(Field field)
    {
        var result = new List<DataField>();
        CollectLeafFields(field, result);
        return result;
    }

    private static void CollectLeafFields(Field field, List<DataField> result)
    {
        switch (field)
        {
            case DataField df:
                result.Add(df);
                break;
            case MapField mf:
                CollectLeafFields(mf.Key, result);
                CollectLeafFields(mf.Value, result);
                break;
            case ListField lf:
                CollectLeafFields(lf.Item, result);
                break;
            case StructField sf:
                foreach (var child in sf.Fields)
                    CollectLeafFields(child, result);
                break;
        }
    }

    /// <summary>
    /// Fallback for complex nested types: produce a flat string representation.
    /// </summary>
    private List<object?> ReconstructFlatFallback(List<DataField> leafFields, Dictionary<string, DataColumn> dataColumns, int rowCount)
    {
        var result = new List<object?>(rowCount);
        for (int r = 0; r < rowCount; r++)
        {
            var obj = new Dictionary<string, object?>();
            foreach (var lf in leafFields)
            {
                if (dataColumns.TryGetValue(lf.Path.ToString(), out var dc) && r < dc.Data.Length)
                    obj[lf.Name] = FormatValue(dc.Data.GetValue(r));
            }
            result.Add(obj.Count > 0 ? obj : null);
        }
        return result;
    }

    public async Task<QueryResult> ExecuteQueryAsync(QueryRequest request)
    {
        EnsureOpen();
        var sw = Stopwatch.StartNew();
        var dataFields = _reader!.Schema.GetDataFields();
        var columns = dataFields.Select(f => f.Name).ToArray();
        var dataTypes = dataFields.Select(f => f.ClrType.Name).ToArray();

        var dynamicPredicates = request.Predicates
            .Select(p => new DynamicPredicate(p.Column, p.Operator, p.Value, p.Value2, p.MaxEdits, p.PrefixLength, p.Transpositions))
            .ToArray();

        var query = DynamicParquetQuery.FromReader(_reader, _currentFilePath);
        if (dynamicPredicates.Length > 0)
            query = query.Where(dynamicPredicates);

        var result = await query.ExecuteAsync(request.Offset, request.Limit);
        sw.Stop();

        var libraryPlan = result.Plan;
        var hasResidualOnly = libraryPlan.PushdownPredicates.Count == 0 && libraryPlan.ResidualPredicates.Count > 0;
        var residualNote = hasResidualOnly
            ? $"Residual filter: {string.Join(", ", libraryPlan.ResidualPredicates)} — all row groups scanned."
            : null;

        var decisions = libraryPlan.RowGroups
            .Select(rg => new RowGroupDecision(
                rg.Index,
                rg.ShouldRead,
                FormatRowGroupReason(rg, residualNote),
                rg.RowCount))
            .ToArray();

        var candidateRows = libraryPlan.RowGroups
            .Where(rg => rg.ShouldRead)
            .Sum(rg => rg.CandidateRowCountUpperBound);

        var plan = new Models.QueryPlan(
            libraryPlan.RowGroups.Count,
            libraryPlan.SelectedRowGroupCount,
            libraryPlan.RowGroups.Count - libraryPlan.SelectedRowGroupCount,
            decisions,
            _totalRowCount,
            candidateRows,
            result.TotalMatchedRows,
            sw.Elapsed.TotalMilliseconds);

        var pagedRows = result.Rows
            .Select(dict => columns.Select(c => FormatValue(dict.TryGetValue(c, out var v) ? v : null)).ToArray())
            .ToArray();

        var data = new DataPage(columns, dataTypes, pagedRows, request.Offset, request.Limit, result.TotalMatchedRows);
        return new QueryResult(plan, data);
    }

    public async Task<Models.QueryPlan> GetQueryPlanAsync(QueryPredicate[] predicates)
    {
        EnsureOpen();
        var sw = Stopwatch.StartNew();

        var dynamicPredicates = predicates
            .Select(p => new DynamicPredicate(p.Column, p.Operator, p.Value, p.Value2, p.MaxEdits, p.PrefixLength, p.Transpositions))
            .ToArray();

        var query = DynamicParquetQuery.FromReader(_reader!, _currentFilePath);
        if (dynamicPredicates.Length > 0)
            query = query.Where(dynamicPredicates);

        var libraryPlan = await query.PlanAsync();
        sw.Stop();

        var hasResidualOnly = libraryPlan.PushdownPredicates.Count == 0 && libraryPlan.ResidualPredicates.Count > 0;
        var residualNote = hasResidualOnly
            ? $"Residual filter: {string.Join(", ", libraryPlan.ResidualPredicates)} — all row groups scanned."
            : null;

        var decisions = libraryPlan.RowGroups
            .Select(rg => new RowGroupDecision(
                rg.Index,
                rg.ShouldRead,
                FormatRowGroupReason(rg, residualNote),
                rg.RowCount))
            .ToArray();

        var candidateRows = libraryPlan.RowGroups
            .Where(rg => rg.ShouldRead)
            .Sum(rg => rg.CandidateRowCountUpperBound);

        return new Models.QueryPlan(
            libraryPlan.RowGroups.Count,
            libraryPlan.SelectedRowGroupCount,
            libraryPlan.RowGroups.Count - libraryPlan.SelectedRowGroupCount,
            decisions,
            _totalRowCount,
            candidateRows,
            -1,
            sw.Elapsed.TotalMilliseconds);
    }

    public IndicesInfo GetIndices()
    {
        EnsureOpen();

        // --- Custom indices from footer metadata ---
        var customIndices = new List<ColumnIndexInfo>();
        var entryCache = new Dictionary<string, IndexEntry[]>(StringComparer.Ordinal);
        var metadata = _reader!.CustomMetadata;
        if (metadata is not null)
        {
            var rgCount = _reader!.RowGroupCount;
            foreach (var kv in metadata)
            {
                if (kv.Key.StartsWith("parquet.query.index.bitmap.v1/"))
                {
                    var col = Uri.UnescapeDataString(kv.Key["parquet.query.index.bitmap.v1/".Length..]);
                    var (stats, entries) = ParseBitmapIndex(kv.Value, rgCount);
                    if (stats is not null)
                        entryCache[$"Bitmap:{col}"] = entries;
                    customIndices.Add(new ColumnIndexInfo(col, "Bitmap",
                        "Low-cardinality equality index. Stores a bitmap of which row groups contain each distinct value. Best for columns with ~256 or fewer distinct values.",
                        ["= (equality)", "!= (inequality)"],
                        stats));
                }
                else if (kv.Key.StartsWith("parquet.query.lucene.v1/"))
                {
                    var col = Uri.UnescapeDataString(kv.Key["parquet.query.lucene.v1/".Length..]);
                    var (stats, entries) = ParseLuceneIndex(kv.Value, rgCount);
                    if (stats is not null)
                        entryCache[$"Lucene:{col}"] = entries;
                    customIndices.Add(new ColumnIndexInfo(col, "Lucene",
                        "Full-text search index with tokenization and optional fuzzy matching (Levenshtein distance 0-2). Prunes row groups by term presence.",
                        ["LuceneMatch (term search)", "LuceneFuzzy (fuzzy matching)"],
                        stats));
                }
            }
        }

        _indexEntryCache = entryCache;

        // --- Built-in optimizations per column ---
        var dataFields = _reader.Schema.GetDataFields();
        var builtinInfo = new List<BuiltinColumnInfo>();

        // Check first row group for per-column metadata (representative)
        if (_reader.RowGroupCount > 0)
        {
            using var rg = _reader.OpenRowGroupReader(0);
            foreach (var field in dataFields)
            {
                if (!rg.ColumnExists(field)) continue;

                var chunk = rg.GetMetadata(field);
                var stats = rg.GetStatistics(field);
                var hasStats = stats?.MinValue is not null || stats?.MaxValue is not null;
                var hasBloom = chunk?.MetaData?.BloomFilterOffset is not null;
                var hasPageIndex = chunk?.ColumnIndexOffset is not null;

                builtinInfo.Add(new BuiltinColumnInfo(
                    field.Path.ToString(),
                    hasStats,
                    hasBloom,
                    hasPageIndex,
                    SortOrder: null)); // filled below from sorting columns
            }
        }

        // --- Sorting columns from row group metadata ---
        var sortingCols = new List<string>();
        if (_reader.RowGroupCount > 0)
        {
            var rgMeta = _reader.Metadata?.RowGroups?[0];
            var sortingColumns = rgMeta?.SortingColumns;
            if (sortingColumns is { Count: > 0 })
            {
                foreach (var sc in sortingColumns)
                {
                    if (sc.ColumnIdx >= 0 && sc.ColumnIdx < dataFields.Length)
                    {
                        var fieldName = dataFields[sc.ColumnIdx].Path.ToString();
                        var dir = sc.Descending ? "DESC" : "ASC";
                        var nulls = sc.NullsFirst ? ", nulls first" : "";
                        sortingCols.Add($"{fieldName} {dir}{nulls}");

                        // Update the builtin entry with sort info
                        var existing = builtinInfo.FindIndex(b => b.ColumnPath == fieldName);
                        if (existing >= 0)
                        {
                            builtinInfo[existing] = builtinInfo[existing] with { SortOrder = $"{dir}{nulls}" };
                        }
                    }
                }
            }
        }

        return new IndicesInfo(
            customIndices.ToArray(),
            builtinInfo.ToArray(),
            sortingCols.ToArray());
    }

    public IndexEntriesPage GetIndexEntries(string columnPath, string indexType, int offset, int limit, string? filter)
    {
        EnsureOpen();

        var cacheKey = $"{indexType}:{columnPath}";
        if (_indexEntryCache is null || !_indexEntryCache.TryGetValue(cacheKey, out var allEntries))
            return new IndexEntriesPage([], 0, offset, limit);

        IEnumerable<IndexEntry> source = allEntries;
        if (!string.IsNullOrWhiteSpace(filter))
            source = source.Where(e => e.Key.Contains(filter, StringComparison.OrdinalIgnoreCase));

        var filtered = source as IndexEntry[] ?? source.ToArray();
        var page = filtered.Skip(offset).Take(limit).ToArray();

        return new IndexEntriesPage(page, filtered.Length, offset, limit);
    }

    private RowGroupInfo[] GetRowGroupsMetadata()
    {
        var dataFields = _reader!.Schema.GetDataFields();
        var rowGroups = new RowGroupInfo[_reader.RowGroupCount];

        for (int rg = 0; rg < _reader.RowGroupCount; rg++)
        {
            using var rgReader = _reader.OpenRowGroupReader(rg);
            var columnChunks = new List<ColumnChunkInfo>();

            foreach (var field in dataFields)
            {
                if (!rgReader.ColumnExists(field)) continue;

                var stats = rgReader.GetStatistics(field);
                var meta = rgReader.GetMetadata(field);

                columnChunks.Add(new ColumnChunkInfo(
                    field.Name,
                    field.ClrType.Name,
                    FormatValue(stats?.MinValue),
                    FormatValue(stats?.MaxValue),
                    stats?.NullCount,
                    stats?.DistinctCount,
                    meta?.MetaData?.Codec.ToString(),
                    meta?.MetaData?.TotalCompressedSize ?? 0,
                    meta?.MetaData?.TotalUncompressedSize ?? 0,
                    meta?.ColumnIndexOffset is not null,
                    meta?.OffsetIndexOffset is not null));
            }

            rowGroups[rg] = new RowGroupInfo(rg, _rowGroupRowCounts![rg], columnChunks.ToArray());
        }

        return rowGroups;
    }

    private FooterInfo GetFooterInfo()
    {
        var kvMeta = new Dictionary<string, string>();
        if (_reader!.CustomMetadata is { } custom)
        {
            foreach (var kv in custom)
                kvMeta[kv.Key] = kv.Value;
        }

        return new FooterInfo(
            _reader.Metadata?.CreatedBy ?? "unknown",
            _reader.Metadata?.Version ?? 0,
            0,
            kvMeta);
    }

    private EncryptionInfo? GetEncryptionInfo()
    {
        if (_currentEncryption is null || string.IsNullOrEmpty(_currentEncryption.FooterKey))
            return null;

        var encryptedColumns = _currentEncryption.ColumnKeys?.Keys.ToArray() ?? [];
        return new EncryptionInfo(
            true,
            !_currentEncryption.PlaintextFooter,
            _currentEncryption.UseCtr ? "AES_GCM_CTR_V1" : "AES_GCM_V1",
            encryptedColumns);
    }

    private ParquetFileInfo BuildFileInfo(string filePath)
    {
        var fi = new FileInfo(filePath);
        var schema = GetSchema();

        var format = "PAR1";
        try
        {
            using var fs = System.IO.File.OpenRead(filePath);
            var magic = new byte[4];
            if (fs.Read(magic, 0, 4) == 4 && magic[0] == 'P' && magic[1] == 'A' && magic[2] == 'R' && magic[3] == 'E')
                format = "PARE";
        }
        catch { /* ignore */ }

        return new ParquetFileInfo(
            filePath,
            fi.Length,
            _reader!.RowGroupCount,
            _totalRowCount,
            schema,
            format == "PARE" || _currentEncryption is not null,
            format);
    }

    private static ParquetOptions BuildOptions(EncryptionConfig? encryption)
    {
        var options = new ParquetOptions();

        if (encryption is null) return options;

        if (!string.IsNullOrEmpty(encryption.FooterKey))
        {
            options.FooterEncryptionKey = NormalizeKey(encryption.FooterKey);
        }

        if (!string.IsNullOrEmpty(encryption.FooterSigningKey))
        {
            options.FooterSigningKey = NormalizeKey(encryption.FooterSigningKey);
        }

        options.UsePlaintextFooter = encryption.PlaintextFooter;
        options.UseCtrVariant = encryption.UseCtr;

        if (!string.IsNullOrEmpty(encryption.AadPrefix))
        {
            options.AADPrefix = encryption.AadPrefix;
        }

        if (encryption.ColumnKeys is { Count: > 0 } columnKeys)
        {
            foreach (var (columnPath, rawKey) in columnKeys)
            {
                options.ColumnKeys[columnPath] = new ParquetOptions.ColumnKeySpec(NormalizeKey(rawKey));
            }
        }

        return options;
    }

    /// <summary>
    /// Normalizes a user-provided key to valid AES key material.
    /// Valid AES keys (UTF-8, hex, or base64) pass through unchanged;
    /// other inputs are SHA-256 hashed and returned as base64.
    /// </summary>
    private static string NormalizeKey(string key)
    {
        if (string.IsNullOrWhiteSpace(key))
            return key;

        if (IsValidUtf8Key(key) || IsValidHexKey(key) || IsValidBase64Key(key))
            return key;

        byte[] hash = System.Security.Cryptography.SHA256.HashData(
            System.Text.Encoding.UTF8.GetBytes(key));
        return Convert.ToBase64String(hash);
    }

    private static bool IsValidUtf8Key(string key)
    {
        int byteCount = System.Text.Encoding.UTF8.GetByteCount(key);
        return byteCount is 16 or 24 or 32;
    }

    private static bool IsValidHexKey(string key)
    {
        if (key.Length is not (32 or 48 or 64))
            return false;
        return key.All(Uri.IsHexDigit);
    }

    private static bool IsValidBase64Key(string key)
    {
        try
        {
            byte[] bytes = Convert.FromBase64String(key);
            return bytes.Length is 16 or 24 or 32;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    private static string FormatRowGroupReason(Parquet.Query.Planning.RowGroupPlan rg, string? residualNote)
    {
        if (rg.Decisions.Count > 0)
            return string.Join(" | ", rg.Decisions.Select(d => $"{d.Predicate}: {d.Reason}"));

        if (residualNote is not null)
            return residualNote;

        return rg.ShouldRead ? "No predicates — read all row groups." : "Ruled out by metadata.";
    }

    private static object? FormatValue(object? value)
    {
        return value switch
        {
            null => null,
            byte[] bytes => Convert.ToBase64String(bytes),
            DateTimeOffset dto => dto.ToString("O"),
            DateTime dt => dt.ToString("O"),
            TimeSpan ts => ts.ToString(),
            _ => value
        };
    }

    private void EnsureOpen()
    {
        if (_reader is null)
            throw new InvalidOperationException("No file is currently open.");
    }

    public void Dispose()
    {
        _reader?.Dispose();
        _reader = null;
        _indexEntryCache = null;
    }

    // --- Footer index payload parsing for stats ---

    private static readonly JsonSerializerOptions s_jsonOptions = new(JsonSerializerDefaults.Web);

    private static T? TryDeserializePayload<T>(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload)) return default;
        try
        {
            var compressedBytes = Convert.FromBase64String(payload);
            using var input = new MemoryStream(compressedBytes, writable: false);
            using var compression = new BrotliStream(input, CompressionMode.Decompress);
            using var json = new MemoryStream();
            compression.CopyTo(json);
            return JsonSerializer.Deserialize<T>(json.ToArray(), s_jsonOptions);
        }
        catch (Exception ex) when (ex is FormatException or JsonException or InvalidDataException or InvalidOperationException)
        {
            return default;
        }
    }

    private static long EstimatePayloadBytes(string base64Payload)
    {
        // Base64 encodes 3 bytes as 4 chars; the raw compressed size is the actual storage cost
        return (long)(base64Payload.Length * 3.0 / 4.0);
    }

    private static (IndexStats? Stats, IndexEntry[] Entries) ParseBitmapIndex(string payload, int rowGroupCount)
    {
        var model = TryDeserializePayload<ViewerBitmapIndexModel>(payload);
        if (model is null) return (null, []);

        var entries = model.Values?
            .Select(v => new IndexEntry(v.Value ?? "", v.RowGroups ?? []))
            .ToArray() ?? [];

        var stats = new IndexStats(
            EstimatePayloadBytes(payload),
            DistinctValueCount: model.Values?.Count ?? 0,
            EntryCount: entries.Length);

        return (stats, entries);
    }

    private static (IndexStats? Stats, IndexEntry[] Entries) ParseLuceneIndex(string payload, int rowGroupCount)
    {
        var model = TryDeserializePayload<ViewerLuceneIndexModel>(payload);
        if (model is null) return (null, []);

        // Build term -> row groups mapping by inverting the per-row-group ordinal lists
        var termRowGroups = new Dictionary<int, List<int>>();
        if (model.RowGroups is not null)
        {
            for (var rgIdx = 0; rgIdx < model.RowGroups.Count; rgIdx++)
            {
                foreach (var ordinal in model.RowGroups[rgIdx])
                {
                    if (!termRowGroups.TryGetValue(ordinal, out var list))
                    {
                        list = [];
                        termRowGroups[ordinal] = list;
                    }
                    list.Add(rgIdx);
                }
            }
        }

        var terms = model.Terms ?? [];
        var entries = new IndexEntry[terms.Count];
        for (var i = 0; i < terms.Count; i++)
        {
            var rgs = termRowGroups.TryGetValue(i, out var list) ? list.ToArray() : [];
            entries[i] = new IndexEntry(terms[i], rgs);
        }

        var stats = new IndexStats(
            EstimatePayloadBytes(payload),
            TermCount: terms.Count,
            EntryCount: entries.Length);

        return (stats, entries);
    }

    private sealed class ViewerBitmapIndexModel
    {
        public string ColumnPath { get; set; } = string.Empty;
        public List<ViewerBitmapEntryModel> Values { get; set; } = [];
    }

    private sealed class ViewerBitmapEntryModel
    {
        public string Value { get; set; } = string.Empty;
        public int[] RowGroups { get; set; } = [];
    }

    private sealed class ViewerLuceneIndexModel
    {
        public string ColumnPath { get; set; } = string.Empty;
        public List<string> Terms { get; set; } = [];
        public List<int[]> RowGroups { get; set; } = [];
    }
}
