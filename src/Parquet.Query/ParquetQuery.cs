using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using Parquet;
using Parquet.Query.Expressions;
using Parquet.Query.Internal;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;
using Parquet.Serialization;

namespace Parquet.Query;

public static class ParquetQuery
{
    public static ParquetQuery<T, T> FromFile<T>(string filePath, ParquetOptions? parquetOptions = null)
        where T : class, new()
        => ParquetQuery<T, T>.FromFiles(new[] { filePath }, parquetOptions);

    public static ParquetQuery<T, T> FromFiles<T>(IEnumerable<string> filePaths, ParquetOptions? parquetOptions = null)
        where T : class, new()
        => ParquetQuery<T, T>.FromFiles(filePaths, parquetOptions);

    public static ParquetQuery<T, T> FromDirectory<T>(
        string directoryPath,
        string searchPattern = "*.parquet",
        SearchOption searchOption = SearchOption.AllDirectories,
        ParquetOptions? parquetOptions = null)
        where T : class, new()
        => ParquetQuery<T, T>.FromFiles(Directory.EnumerateFiles(directoryPath, searchPattern, searchOption), parquetOptions);
}

public sealed class ParquetQuery<TSource, TResult>
    where TSource : class, new()
{
    private readonly IReadOnlyList<string> _filePaths;
    private readonly ParquetOptions? _parquetOptions;
    private readonly PushdownFilter<TSource> _pushdownFilter;
    private readonly IReadOnlyList<Expression<Func<TSource, bool>>> _wherePredicates;
    private readonly IReadOnlyList<string> _residualPredicates;
    private readonly Expression<Func<TSource, TResult>>? _projection;
    private readonly bool _strictPushdown;

    private ParquetQuery(
        IReadOnlyList<string> filePaths,
        ParquetOptions? parquetOptions,
        PushdownFilter<TSource> pushdownFilter,
        IReadOnlyList<Expression<Func<TSource, bool>>> wherePredicates,
        IReadOnlyList<string> residualPredicates,
        Expression<Func<TSource, TResult>>? projection,
        bool strictPushdown)
    {
        _filePaths = filePaths;
        _parquetOptions = parquetOptions;
        _pushdownFilter = pushdownFilter;
        _wherePredicates = wherePredicates;
        _residualPredicates = residualPredicates;
        _projection = projection;
        _strictPushdown = strictPushdown;
    }

    public static ParquetQuery<TSource, TResult> FromFiles(IEnumerable<string> filePaths, ParquetOptions? parquetOptions = null)
    {
        ArgumentNullException.ThrowIfNull(filePaths);

        var normalizedFilePaths = filePaths
            .Where(path => !string.IsNullOrWhiteSpace(path))
            .Select(path => Path.GetFullPath(path))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (normalizedFilePaths.Length == 0)
        {
            throw new ArgumentException("At least one parquet file must be provided.", nameof(filePaths));
        }

        return new ParquetQuery<TSource, TResult>(
            normalizedFilePaths,
            parquetOptions,
            PushdownFilter<TSource>.Empty,
            Array.Empty<Expression<Func<TSource, bool>>>(),
            Array.Empty<string>(),
            projection: null,
            strictPushdown: false);
    }

    public ParquetQuery<TSource, TResult> Pushdown(PushdownFilter<TSource> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter.And(filter),
            _wherePredicates,
            _residualPredicates,
            _projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TResult> Pushdown(Func<PushdownFilterBuilder<TSource>, PushdownFilterBuilder<TSource>> configure) =>
        Pushdown(global::Parquet.Query.Pushdown.Pushdown.For(configure));

    public ParquetQuery<TSource, TResult> Where(Expression<Func<TSource, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var split = PredicatePushdownExtractor.Extract(predicate);

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter.And(split.PushdownFilter),
            new ReadOnlyCollection<Expression<Func<TSource, bool>>>(_wherePredicates.Concat(new[] { predicate }).ToArray()),
            new ReadOnlyCollection<string>(_residualPredicates.Concat(split.UnsupportedExpressions).ToArray()),
            _projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TNextResult> Select<TNextResult>(Expression<Func<TSource, TNextResult>> projection)
    {
        ArgumentNullException.ThrowIfNull(projection);

        return new ParquetQuery<TSource, TNextResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter,
            _wherePredicates,
            _residualPredicates,
            projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TResult> StrictPushdown(bool enabled = true) =>
        new(
            _filePaths,
            _parquetOptions,
            _pushdownFilter,
            _wherePredicates,
            _residualPredicates,
            _projection,
            enabled);

    public ParquetQuery<TSource, TResult> WithParquetOptions(ParquetOptions parquetOptions)
    {
        ArgumentNullException.ThrowIfNull(parquetOptions);

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            ParquetOptionsFactory.Clone(parquetOptions),
            _pushdownFilter,
            _wherePredicates,
            _residualPredicates,
            _projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TResult> ConfigureParquetOptions(Action<ParquetOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = ParquetOptionsFactory.Clone(_parquetOptions);
        configure(options);
        return WithParquetOptions(options);
    }

    public ParquetQuery<TSource, TResult> WithFooterKey(string footerEncryptionKey, byte[]? keyMetadata = null) =>
        ConfigureParquetOptions(options =>
        {
            options.FooterEncryptionKey = footerEncryptionKey;
            options.FooterEncryptionKeyMetadata = keyMetadata?.ToArray();
        });

    public ParquetQuery<TSource, TResult> WithFooterSigningKey(string footerSigningKey, byte[]? keyMetadata = null) =>
        ConfigureParquetOptions(options =>
        {
            options.FooterSigningKey = footerSigningKey;
            options.FooterSigningKeyMetadata = keyMetadata?.ToArray();
        });

    public ParquetQuery<TSource, TResult> UsePlaintextFooter(bool enabled = true) =>
        ConfigureParquetOptions(options => options.UsePlaintextFooter = enabled);

    public ParquetQuery<TSource, TResult> WithAadPrefix(string aadPrefix, bool supplyOutOfBand = false) =>
        ConfigureParquetOptions(options =>
        {
            options.AADPrefix = aadPrefix;
            options.SupplyAadPrefix = supplyOutOfBand;
        });

    public ParquetQuery<TSource, TResult> UseCtrVariant(bool enabled = true) =>
        ConfigureParquetOptions(options => options.UseCtrVariant = enabled);

    public ParquetQuery<TSource, TResult> WithColumnKeyResolver(Func<IReadOnlyList<string>, byte[]?, string?> resolver) =>
        ConfigureParquetOptions(options => options.ColumnKeyResolver = resolver);

    public async Task<ParquetQueryPlan> PlanAsync(CancellationToken cancellationToken = default)
    {
        EnsureStrictPushdown();

        var filePlans = new List<QueryFilePlan>(_filePaths.Count);
        var readColumns = new HashSet<string>(StringComparer.Ordinal);
        var filterColumns = new HashSet<string>(StringComparer.Ordinal);
        var deferredColumns = new HashSet<string>(StringComparer.Ordinal);
        var requiresFullMaterialization = false;

        foreach (var filePath in _filePaths)
        {
            var fileDecisions = PartitionPruner.Evaluate(filePath, _pushdownFilter.Predicates);
            if (fileDecisions.Any(decision => !decision.MayMatch))
            {
                filePlans.Add(new QueryFilePlan(
                    filePath,
                    shouldRead: false,
                    reason: "Path partitions ruled the file out.",
                    decisions: fileDecisions,
                    rowGroups: Array.Empty<RowGroupPlan>(),
                    pageIndexAvailable: false));
                continue;
            }

            await using var stream = System.IO.File.OpenRead(filePath);
            using var reader = await ParquetReader.CreateAsync(stream, _parquetOptions, leaveStreamOpen: false, cancellationToken);
            var materializationPlan = CreateMaterializationPlan(reader.Schema);
            requiresFullMaterialization |= materializationPlan.RequiresFullMaterialization;
            readColumns.UnionWith(materializationPlan.RequiredColumnPaths);
            filterColumns.UnionWith(materializationPlan.FilterColumnPaths);
            deferredColumns.UnionWith(materializationPlan.DeferredColumnPaths);

            filePlans.Add(await RowGroupPlanner.BuildFilePlanAsync(filePath, reader, _pushdownFilter, fileDecisions, cancellationToken));
        }

        return new ParquetQueryPlan(
            filePlans,
            _pushdownFilter.Predicates.Select(predicate => predicate.Description).ToArray(),
            _residualPredicates,
            readColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray(),
            filterColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray(),
            deferredColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray(),
            requiresFullMaterialization);
    }

    public async Task<string> ExplainAsync(CancellationToken cancellationToken = default)
    {
        var plan = await PlanAsync(cancellationToken);
        var builder = new StringBuilder();

        builder.AppendLine(plan.Files.Count == 1
            ? $"File: {plan.Files[0].FilePath}"
            : $"Files: {plan.SelectedFileCount}/{plan.Files.Count} selected");
        builder.AppendLine(plan.PushdownPredicates.Count == 0
            ? "Pushdown: none"
            : $"Pushdown: {string.Join(", ", plan.PushdownPredicates)}");
        builder.AppendLine(plan.ResidualPredicates.Count == 0
            ? "Residual: none"
            : $"Residual: {string.Join(", ", plan.ResidualPredicates)}");
        builder.AppendLine(plan.RequiresFullMaterialization
            ? "Read Columns: all"
            : $"Read Columns: {string.Join(", ", plan.ReadColumns)}");
        if (!plan.RequiresFullMaterialization)
        {
            builder.AppendLine(plan.FilterColumns.Count == 0
                ? "Filter Columns: none"
                : $"Filter Columns: {string.Join(", ", plan.FilterColumns)}");
            builder.AppendLine(plan.DeferredColumns.Count == 0
                ? "Deferred Columns: none"
                : $"Deferred Columns: {string.Join(", ", plan.DeferredColumns)}");
        }

        builder.AppendLine(plan.UsesLateMaterialization
            ? "Late Materialization: enabled"
            : "Late Materialization: not needed");

        builder.AppendLine(!plan.PageIndexAvailable
            ? "Page Indexes: unavailable"
            : plan.UsedFallbackPageIndex
                ? $"Page Indexes: available, using persisted and fallback indexes where needed ({plan.SelectedPageCount}/{plan.TotalPageCount} pages selected)."
                : $"Page Indexes: available from persisted metadata ({plan.SelectedPageCount}/{plan.TotalPageCount} pages selected).");

        foreach (var file in plan.Files)
        {
            builder.AppendLine($"File {file.FilePath}: {(file.ShouldRead ? "read" : "skip")} ({file.Reason})");
            foreach (var decision in file.Decisions)
            {
                builder.AppendLine($"  Partition {decision.Predicate}: {(decision.MayMatch ? "may match" : "ruled out")} via {decision.Source} ({decision.Reason})");
            }

            foreach (var rowGroup in file.RowGroups)
            {
                var pageDetails = rowGroup.PageCount > 0
                    ? $", pages {rowGroup.SelectedPageCount}/{rowGroup.PageCount}, candidate rows <= {rowGroup.CandidateRowCountUpperBound}"
                    : string.Empty;
                builder.AppendLine($"  RG {rowGroup.Index}: {(rowGroup.ShouldRead ? "read" : "skip")} ({rowGroup.RowCount} rows{pageDetails})");
                if (rowGroup.PageCount > 0 || rowGroup.PageIndexAvailable)
                {
                    builder.AppendLine($"    page pruning: {rowGroup.PagePruningSource} ({rowGroup.PagePruningReason})");
                }

                foreach (var decision in rowGroup.Decisions)
                {
                    builder.AppendLine($"    {decision.Predicate}: {(decision.MayMatch ? "may match" : "ruled out")} via {decision.Source} ({decision.Reason})");
                }
            }
        }

        return builder.ToString().TrimEnd();
    }

    public async Task<IReadOnlyList<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        EnsureStrictPushdown();

        var rowFilter = BuildRowFilter();
        var projector = BuildProjector();
        var results = new List<TResult>();

        foreach (var filePath in _filePaths)
        {
            var fileDecisions = PartitionPruner.Evaluate(filePath, _pushdownFilter.Predicates);
            if (fileDecisions.Any(decision => !decision.MayMatch))
            {
                continue;
            }

            await using var stream = System.IO.File.OpenRead(filePath);
            using var reader = await ParquetReader.CreateAsync(stream, _parquetOptions, leaveStreamOpen: false, cancellationToken);
            var materializationPlan = CreateMaterializationPlan(reader.Schema);
            var serializerOptions = CreateSerializerOptions();
            var filePlan = await RowGroupPlanner.BuildFilePlanAsync(filePath, reader, _pushdownFilter, fileDecisions, cancellationToken);
            if (!filePlan.ShouldRead)
            {
                continue;
            }

            foreach (var rowGroup in filePlan.RowGroups.Where(rowGroup => rowGroup.ShouldRead))
            {
                PagePruningResult pagePruning;
                using (var rowGroupReader = reader.OpenRowGroupReader(rowGroup.Index))
                {
                    pagePruning = await PagePruner.PruneAsync(rowGroupReader, reader.Schema, _pushdownFilter, cancellationToken);
                }

                if (pagePruning.CandidateRowCountUpperBound == 0)
                {
                    continue;
                }

                if (materializationPlan.RequiresFullMaterialization)
                {
                    var batch = await PartialRowMaterializer<TSource>.ReadRowGroupAsync(
                        filePath,
                        reader,
                        rowGroup.Index,
                        materializationPlan,
                        serializerOptions,
                        pagePruning.Intervals,
                        cancellationToken);

                    foreach (var row in batch)
                    {
                        if (rowFilter(row))
                        {
                            results.Add(projector(row));
                        }
                    }

                    continue;
                }

                var rowSet = await PartialRowMaterializer<TSource>.ReadFilterRowsAsync(
                    reader,
                    rowGroup.Index,
                    materializationPlan,
                    pagePruning.Intervals,
                    cancellationToken);

                var selectedIndexes = SelectMatchingIndexes(rowSet.Rows, rowFilter);
                if (selectedIndexes.Count == 0)
                {
                    continue;
                }

                await PartialRowMaterializer<TSource>.PopulateDeferredColumnsAsync(
                    reader,
                    rowGroup.Index,
                    materializationPlan,
                    rowSet,
                    selectedIndexes,
                    cancellationToken);

                foreach (var selectedIndex in selectedIndexes)
                {
                    results.Add(projector(rowSet.Rows[selectedIndex]));
                }
            }
        }

        return results;
    }

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureStrictPushdown();

        var rowFilter = BuildRowFilter();
        var projector = BuildProjector();

        foreach (var filePath in _filePaths)
        {
            var fileDecisions = PartitionPruner.Evaluate(filePath, _pushdownFilter.Predicates);
            if (fileDecisions.Any(decision => !decision.MayMatch))
            {
                continue;
            }

            await using var stream = System.IO.File.OpenRead(filePath);
            using var reader = await ParquetReader.CreateAsync(stream, _parquetOptions, leaveStreamOpen: false, cancellationToken);
            var materializationPlan = CreateMaterializationPlan(reader.Schema);
            var serializerOptions = CreateSerializerOptions();
            var filePlan = await RowGroupPlanner.BuildFilePlanAsync(filePath, reader, _pushdownFilter, fileDecisions, cancellationToken);
            if (!filePlan.ShouldRead)
            {
                continue;
            }

            foreach (var rowGroup in filePlan.RowGroups.Where(rowGroup => rowGroup.ShouldRead))
            {
                PagePruningResult pagePruning;
                using (var rowGroupReader = reader.OpenRowGroupReader(rowGroup.Index))
                {
                    pagePruning = await PagePruner.PruneAsync(rowGroupReader, reader.Schema, _pushdownFilter, cancellationToken);
                }

                if (pagePruning.CandidateRowCountUpperBound == 0)
                {
                    continue;
                }

                if (materializationPlan.RequiresFullMaterialization)
                {
                    var batch = await PartialRowMaterializer<TSource>.ReadRowGroupAsync(
                        filePath,
                        reader,
                        rowGroup.Index,
                        materializationPlan,
                        serializerOptions,
                        pagePruning.Intervals,
                        cancellationToken);

                    foreach (var row in batch)
                    {
                        if (rowFilter(row))
                        {
                            yield return projector(row);
                        }
                    }

                    continue;
                }

                var rowSet = await PartialRowMaterializer<TSource>.ReadFilterRowsAsync(
                    reader,
                    rowGroup.Index,
                    materializationPlan,
                    pagePruning.Intervals,
                    cancellationToken);

                var selectedIndexes = SelectMatchingIndexes(rowSet.Rows, rowFilter);
                if (selectedIndexes.Count == 0)
                {
                    continue;
                }

                await PartialRowMaterializer<TSource>.PopulateDeferredColumnsAsync(
                    reader,
                    rowGroup.Index,
                    materializationPlan,
                    rowSet,
                    selectedIndexes,
                    cancellationToken);

                foreach (var selectedIndex in selectedIndexes)
                {
                    yield return projector(rowSet.Rows[selectedIndex]);
                }
            }
        }
    }

    private SourceMaterializationPlan<TSource> CreateMaterializationPlan(Parquet.Schema.ParquetSchema schema) =>
        SourceMaterializationPlanBuilder.Build<TSource, TResult>(
            schema,
            _pushdownFilter,
            _wherePredicates,
            _projection);

    private Func<TSource, bool> BuildRowFilter()
    {
        var whereDelegates = _wherePredicates.Select(predicate => predicate.Compile()).ToArray();
        if (_pushdownFilter.IsEmpty && whereDelegates.Length == 0)
        {
            return static _ => true;
        }

        return row => _pushdownFilter.Matches(row) && whereDelegates.All(predicate => predicate(row));
    }

    private Func<TSource, TResult> BuildProjector()
    {
        if (_projection is not null)
        {
            return _projection.Compile();
        }

        return static row => (TResult)(object)row;
    }

    private static IReadOnlyList<int> SelectMatchingIndexes(
        IReadOnlyList<TSource> rows,
        Func<TSource, bool> rowFilter)
    {
        var selectedIndexes = new List<int>(rows.Count);
        for (var index = 0; index < rows.Count; index++)
        {
            if (rowFilter(rows[index]))
            {
                selectedIndexes.Add(index);
            }
        }

        return selectedIndexes;
    }

    private ParquetSerializerOptions? CreateSerializerOptions() =>
        _parquetOptions is null
            ? null
            : new ParquetSerializerOptions
            {
                ParquetOptions = _parquetOptions
            };

    private void EnsureStrictPushdown()
    {
        if (_strictPushdown && _residualPredicates.Count > 0)
        {
            throw new InvalidOperationException(
                "Strict pushdown was enabled, but the following predicates could not be translated: " +
                string.Join(", ", _residualPredicates));
        }
    }
}
