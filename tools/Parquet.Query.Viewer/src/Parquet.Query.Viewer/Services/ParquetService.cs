using System.Diagnostics;
using Parquet;
using Parquet.Data;
using Parquet.Meta;
using Parquet.Schema;
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

    public bool HasOpenFile => _reader is not null;
    public string? CurrentFilePath => _currentFilePath;

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
        var fields = _reader!.Schema.GetDataFields();
        return new SchemaInfo(fields.Select(f => new ColumnInfo(
            f.Name,
            f.Path.ToString(),
            f.ClrType.Name,
            f.ClrType.Name,
            f.IsNullable,
            f.IsArray
        )).ToArray());
    }

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
        var dataFields = _reader!.Schema.GetDataFields();
        var columns = dataFields.Select(f => f.Name).ToArray();
        var dataTypes = dataFields.Select(f => f.ClrType.Name).ToArray();
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
            var columnArrays = new Dictionary<string, Array>();
            foreach (var field in dataFields)
            {
                if (rgReader.ColumnExists(field))
                {
                    var col = await rgReader.ReadColumnAsync(field);
                    columnArrays[field.Name] = col.Data;
                }
            }

            var rgStart = (int)Math.Max(0, offset - currentRow);
            var rgEnd = (int)Math.Min(rgRowCount, offset + limit - currentRow);

            for (int i = rgStart; i < rgEnd && rows.Count < limit; i++)
            {
                var row = new object?[dataFields.Length];
                for (int c = 0; c < dataFields.Length; c++)
                {
                    if (columnArrays.TryGetValue(dataFields[c].Name, out var arr) && i < arr.Length)
                    {
                        row[c] = FormatValue(arr.GetValue(i));
                    }
                }
                rows.Add(row);
            }

            currentRow += rgRowCount;
        }

        return new DataPage(columns, dataTypes, rows.ToArray(), offset, limit, _totalRowCount);
    }

    public async Task<QueryResult> ExecuteQueryAsync(QueryRequest request)
    {
        EnsureOpen();
        var sw = Stopwatch.StartNew();
        var evaluator = new PredicateEvaluator();
        var dataFields = _reader!.Schema.GetDataFields();

        var decisions = new List<RowGroupDecision>();
        long candidateRows = 0;

        for (int rg = 0; rg < _reader.RowGroupCount; rg++)
        {
            using var rgReader = _reader.OpenRowGroupReader(rg);
            var rowCount = _rowGroupRowCounts![rg];
            var (shouldRead, reason) = evaluator.EvaluateRowGroup(rgReader, dataFields, request.Predicates);

            if (shouldRead) candidateRows += rowCount;
            decisions.Add(new RowGroupDecision(rg, shouldRead, reason, rowCount));
        }

        var columns = dataFields.Select(f => f.Name).ToArray();
        var dataTypes = dataFields.Select(f => f.ClrType.Name).ToArray();
        var allMatchedRows = new List<object?[]>();

        for (int rg = 0; rg < _reader.RowGroupCount; rg++)
        {
            if (!decisions[rg].ShouldRead) continue;

            using var rgReader = _reader.OpenRowGroupReader(rg);
            var columnArrays = new Dictionary<string, Array>();
            foreach (var field in dataFields)
            {
                if (rgReader.ColumnExists(field))
                {
                    var col = await rgReader.ReadColumnAsync(field);
                    columnArrays[field.Name] = col.Data;
                }
            }

            var rgRowCount = (int)_rowGroupRowCounts![rg];
            for (int i = 0; i < rgRowCount; i++)
            {
                var rowValues = new Dictionary<string, object?>();
                for (int c = 0; c < dataFields.Length; c++)
                {
                    if (columnArrays.TryGetValue(dataFields[c].Name, out var arr) && i < arr.Length)
                        rowValues[dataFields[c].Name] = arr.GetValue(i);
                    else
                        rowValues[dataFields[c].Name] = null;
                }

                if (evaluator.MatchesAllPredicates(rowValues, dataFields, request.Predicates))
                {
                    var row = new object?[dataFields.Length];
                    for (int c = 0; c < dataFields.Length; c++)
                        row[c] = FormatValue(rowValues[dataFields[c].Name]);
                    allMatchedRows.Add(row);
                }
            }
        }

        sw.Stop();

        var pagedRows = allMatchedRows.Skip(request.Offset).Take(request.Limit).ToArray();

        var plan = new QueryPlan(
            _reader.RowGroupCount,
            decisions.Count(d => d.ShouldRead),
            decisions.Count(d => !d.ShouldRead),
            decisions.ToArray(),
            _totalRowCount,
            candidateRows,
            allMatchedRows.Count,
            sw.Elapsed.TotalMilliseconds);

        var data = new DataPage(columns, dataTypes, pagedRows, request.Offset, request.Limit, allMatchedRows.Count);
        return new QueryResult(plan, data);
    }

    public QueryPlan GetQueryPlan(QueryPredicate[] predicates)
    {
        EnsureOpen();
        var sw = Stopwatch.StartNew();
        var evaluator = new PredicateEvaluator();
        var dataFields = _reader!.Schema.GetDataFields();
        var decisions = new List<RowGroupDecision>();
        long candidateRows = 0;

        for (int rg = 0; rg < _reader.RowGroupCount; rg++)
        {
            using var rgReader = _reader.OpenRowGroupReader(rg);
            var rowCount = _rowGroupRowCounts![rg];
            var (shouldRead, reason) = evaluator.EvaluateRowGroup(rgReader, dataFields, predicates);

            if (shouldRead) candidateRows += rowCount;
            decisions.Add(new RowGroupDecision(rg, shouldRead, reason, rowCount));
        }

        sw.Stop();
        return new QueryPlan(
            _reader.RowGroupCount,
            decisions.Count(d => d.ShouldRead),
            decisions.Count(d => !d.ShouldRead),
            decisions.ToArray(),
            _totalRowCount,
            candidateRows,
            -1,
            sw.Elapsed.TotalMilliseconds);
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
                    meta?.MetaData?.TotalUncompressedSize ?? 0));
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
            options.FooterEncryptionKey = encryption.FooterKey;
        }

        if (!string.IsNullOrEmpty(encryption.FooterSigningKey))
        {
            options.FooterSigningKey = encryption.FooterSigningKey;
        }

        options.UsePlaintextFooter = encryption.PlaintextFooter;
        options.UseCtrVariant = encryption.UseCtr;

        if (!string.IsNullOrEmpty(encryption.AadPrefix))
        {
            options.AADPrefix = encryption.AadPrefix;
        }

        if (encryption.ColumnKeys is { Count: > 0 } columnKeys)
        {
            foreach (var (columnPath, hexKey) in columnKeys)
            {
                options.ColumnKeys[columnPath] = new ParquetOptions.ColumnKeySpec(hexKey);
            }
        }

        return options;
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
    }
}
