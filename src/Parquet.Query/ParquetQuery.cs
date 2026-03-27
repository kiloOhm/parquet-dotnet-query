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
    private static readonly StringComparer FilePathComparer = OperatingSystem.IsWindows()
        ? StringComparer.OrdinalIgnoreCase
        : StringComparer.Ordinal;

    private readonly IReadOnlyList<string> _filePaths;
    private readonly ParquetOptions? _parquetOptions;
    private readonly PushdownFilter<TSource> _pushdownFilter;
    private readonly IReadOnlyList<IParquetPredicatePlanner<TSource>> _predicatePlanners;
    private readonly IReadOnlyList<Expression<Func<TSource, bool>>> _wherePredicates;
    private readonly Lazy<Func<TSource, bool>[]> _compiledWherePredicates;
    private readonly IReadOnlyList<PredicatePushdownDiagnostic> _residualPredicates;
    private readonly Expression<Func<TSource, TResult>>? _projection;
    private readonly bool _strictPushdown;

    private ParquetQuery(
        IReadOnlyList<string> filePaths,
        ParquetOptions? parquetOptions,
        PushdownFilter<TSource> pushdownFilter,
        IReadOnlyList<IParquetPredicatePlanner<TSource>> predicatePlanners,
        IReadOnlyList<Expression<Func<TSource, bool>>> wherePredicates,
        IReadOnlyList<PredicatePushdownDiagnostic> residualPredicates,
        Expression<Func<TSource, TResult>>? projection,
        bool strictPushdown)
    {
        _filePaths = filePaths;
        _parquetOptions = parquetOptions;
        _pushdownFilter = pushdownFilter;
        _predicatePlanners = predicatePlanners;
        _wherePredicates = wherePredicates;
        _compiledWherePredicates = new Lazy<Func<TSource, bool>[]>(
            () => _wherePredicates.Select(predicate => predicate.Compile()).ToArray(),
            isThreadSafe: true);
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
            .Distinct(FilePathComparer)
            .OrderBy(path => path, FilePathComparer)
            .ToArray();

        if (normalizedFilePaths.Length == 0)
        {
            throw new ArgumentException("At least one parquet file must be provided.", nameof(filePaths));
        }

        return new ParquetQuery<TSource, TResult>(
            normalizedFilePaths,
            parquetOptions,
            PushdownFilter<TSource>.Empty,
            Array.Empty<IParquetPredicatePlanner<TSource>>(),
            Array.Empty<Expression<Func<TSource, bool>>>(),
            Array.Empty<PredicatePushdownDiagnostic>(),
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
            _predicatePlanners,
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
            _predicatePlanners,
            new ReadOnlyCollection<Expression<Func<TSource, bool>>>(_wherePredicates.Concat(new[] { predicate }).ToArray()),
            new ReadOnlyCollection<PredicatePushdownDiagnostic>(_residualPredicates.Concat(split.Diagnostics).ToArray()),
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
            _predicatePlanners,
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
            _predicatePlanners,
            _wherePredicates,
            _residualPredicates,
            _projection,
            enabled);

    public ParquetQuery<TSource, TResult> WithPredicatePlanner(IParquetPredicatePlanner<TSource> predicatePlanner)
    {
        ArgumentNullException.ThrowIfNull(predicatePlanner);
        return WithPredicatePlanners(new[] { predicatePlanner });
    }

    public ParquetQuery<TSource, TResult> WithPredicatePlanners(IEnumerable<IParquetPredicatePlanner<TSource>> predicatePlanners)
    {
        ArgumentNullException.ThrowIfNull(predicatePlanners);

        var combinedPlanners = _predicatePlanners
            .Concat(predicatePlanners)
            .Where(planner => planner is not null)
            .ToArray();

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter,
            new ReadOnlyCollection<IParquetPredicatePlanner<TSource>>(combinedPlanners),
            _wherePredicates,
            _residualPredicates,
            _projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TResult> WithParquetOptions(ParquetOptions parquetOptions)
    {
        ArgumentNullException.ThrowIfNull(parquetOptions);

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            ParquetOptionsFactory.Clone(parquetOptions),
            _pushdownFilter,
            _predicatePlanners,
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
        var executionPlan = await BuildExecutionPlanAsync(cancellationToken);
        return executionPlan.QueryPlan;
    }

    public async Task<string> ExplainAsync(CancellationToken cancellationToken = default)
    {
        var executionPlan = await BuildExecutionPlanAsync(cancellationToken);
        var plan = executionPlan.QueryPlan;
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
        if (plan.ResidualPredicateDiagnostics.Count > 0)
        {
            builder.AppendLine("Residual Diagnostics:");
            foreach (var diagnostic in plan.ResidualPredicateDiagnostics)
            {
                builder.AppendLine($"  {diagnostic}");
            }
        }
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

    public async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        if (_pushdownFilter.IsEmpty && _wherePredicates.Count == 0)
        {
            var executionPlan = await BuildExecutionPlanAsync(cancellationToken, countOnly: true);
            return executionPlan.Files
                .Where(file => file.FilePlan.ShouldRead)
                .Sum(file => file.FilePlan.RowGroups.Where(rowGroup => rowGroup.ShouldRead).Sum(rowGroup => rowGroup.RowCount));
        }

        long count = 0;
        await foreach (var matchCount in EnumerateMatchCountsAsync(cancellationToken))
        {
            count += matchCount;
        }

        return count;
    }

    public Task<long> LongCountAsync(CancellationToken cancellationToken = default) =>
        CountAsync(cancellationToken);

    public async Task<bool> AnyAsync(CancellationToken cancellationToken = default)
    {
        if (_pushdownFilter.IsEmpty && _wherePredicates.Count == 0)
        {
            var executionPlan = await BuildExecutionPlanAsync(cancellationToken, countOnly: true);
            return executionPlan.Files.Any(file =>
                file.FilePlan.ShouldRead &&
                file.FilePlan.RowGroups.Any(rowGroup => rowGroup.ShouldRead && rowGroup.RowCount > 0));
        }

        await foreach (var matchCount in EnumerateMatchCountsAsync(cancellationToken))
        {
            if (matchCount > 0)
            {
                return true;
            }
        }

        return false;
    }

    public async Task<TResult?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        await foreach (var result in EnumerateResultsAsync(cancellationToken))
        {
            return result;
        }

        return default;
    }

    public async Task<IReadOnlyList<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var results = new List<TResult>();
        await foreach (var result in EnumerateResultsAsync(cancellationToken))
        {
            results.Add(result);
        }

        return results;
    }

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var result in EnumerateResultsAsync(cancellationToken))
        {
            yield return result;
        }
    }

    private async IAsyncEnumerable<int> EnumerateMatchCountsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var rowFilter = BuildRowFilter();

        await foreach (var openFile in OpenExecutionFilesAsync(countOnly: true, cancellationToken))
        {
            var file = openFile.ExecutionFilePlan;
            var reader = openFile.Reader;
            var materializationPlan = file.MaterializationPlan!;
            var serializerOptions = openFile.SerializerOptions;

            foreach (var rowGroup in file.FilePlan.RowGroups.Where(rowGroup => rowGroup.ShouldRead && rowGroup.CandidateRowCountUpperBound > 0))
            {
                if (materializationPlan.RequiresFullMaterialization)
                {
                    var batch = await PartialRowMaterializer<TSource>.ReadRowGroupAsync(
                        reader,
                        rowGroup.Index,
                        materializationPlan,
                        serializerOptions,
                        rowGroup.CandidateIntervals,
                        cancellationToken);
                    yield return CountMatchingRows(batch, rowFilter);
                    continue;
                }

                var rowSet = await PartialRowMaterializer<TSource>.ReadFilterRowsAsync(
                    reader,
                    rowGroup.Index,
                    materializationPlan,
                    rowGroup.CandidateIntervals,
                    cancellationToken);
                yield return CountMatchingRows(rowSet.Rows, rowFilter);
            }
        }
    }

    private async IAsyncEnumerable<TResult> EnumerateResultsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var rowFilter = BuildRowFilter();
        var isRowFilterTrivial = _pushdownFilter.IsEmpty && _wherePredicates.Count == 0;

        await foreach (var openFile in OpenExecutionFilesAsync(countOnly: false, cancellationToken))
        {
            var file = openFile.ExecutionFilePlan;
            var reader = openFile.Reader;
            var materializationPlan = file.MaterializationPlan!;
            var projectionPlan = ProjectionPlan<TSource, TResult>.Create(_projection, materializationPlan);
            var serializerOptions = openFile.SerializerOptions;

            foreach (var rowGroup in file.FilePlan.RowGroups.Where(rowGroup => rowGroup.ShouldRead))
            {
                if (rowGroup.CandidateRowCountUpperBound == 0)
                {
                    continue;
                }

                if (!materializationPlan.RequiresFullMaterialization &&
                    projectionPlan.IsDirectScalar &&
                    isRowFilterTrivial)
                {
                    foreach (var result in await ReadProjectedValuesAsync(reader, rowGroup.Index, rowGroup.RowCount, serializerOptions, projectionPlan, rowGroup.CandidateIntervals, cancellationToken))
                    {
                        yield return result;
                    }

                    continue;
                }

                if (materializationPlan.RequiresFullMaterialization)
                {
                    var batch = await PartialRowMaterializer<TSource>.ReadRowGroupAsync(
                        reader,
                        rowGroup.Index,
                        materializationPlan,
                        serializerOptions,
                        rowGroup.CandidateIntervals,
                        cancellationToken);

                    foreach (var row in batch)
                    {
                        if (rowFilter(row))
                        {
                            yield return projectionPlan.ProjectRow(row);
                        }
                    }

                    continue;
                }

                var rowSet = await PartialRowMaterializer<TSource>.ReadFilterRowsAsync(
                    reader,
                    rowGroup.Index,
                    materializationPlan,
                    rowGroup.CandidateIntervals,
                    cancellationToken);

                var selectedIndexes = SelectMatchingIndexes(rowSet.Rows, rowFilter);
                if (selectedIndexes.Count == 0)
                {
                    continue;
                }

                if (projectionPlan.IsDirectScalar)
                {
                    foreach (var result in await ProjectDirectScalarResultsAsync(reader, rowGroup.Index, serializerOptions, materializationPlan, projectionPlan, rowSet, selectedIndexes, cancellationToken))
                    {
                        yield return result;
                    }

                    continue;
                }

                if (projectionPlan.IsVectorized)
                {
                    foreach (var result in await ProjectVectorizedResultsAsync(reader, rowGroup.Index, serializerOptions, materializationPlan, projectionPlan, rowSet, selectedIndexes, cancellationToken))
                    {
                        yield return result;
                    }

                    continue;
                }

                if (materializationPlan.DeferredBindings.Any(binding => binding.RequiresFullRowRead))
                {
                    foreach (var result in await ProjectFromFullRowsAsync(rowGroup.Index, reader, serializerOptions, projectionPlan, materializationPlan, rowSet, selectedIndexes, cancellationToken))
                    {
                        yield return result;
                    }

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
                    yield return projectionPlan.ProjectRow(rowSet.Rows[selectedIndex]);
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

    private SourceMaterializationPlan<TSource> CreateCountMaterializationPlan(Parquet.Schema.ParquetSchema schema) =>
        SourceMaterializationPlanBuilder.Build(
            schema,
            _pushdownFilter,
            _wherePredicates,
            projection: null,
            includeDefaultResultPaths: false);

    private Func<TSource, bool> BuildRowFilter()
    {
        var whereDelegates = _compiledWherePredicates.Value;
        if (_pushdownFilter.IsEmpty && whereDelegates.Length == 0)
        {
            return static _ => true;
        }

        return row => _pushdownFilter.Matches(row) && whereDelegates.All(predicate => predicate(row));
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

    private static int CountMatchingRows(
        IReadOnlyList<TSource> rows,
        Func<TSource, bool> rowFilter)
    {
        var count = 0;
        for (var index = 0; index < rows.Count; index++)
        {
            if (rowFilter(rows[index]))
            {
                count++;
            }
        }

        return count;
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
                string.Join(", ", _residualPredicates.Select(diagnostic => diagnostic.ToString())));
        }
    }

    private async IAsyncEnumerable<OpenQueryExecutionFile<TSource>> OpenExecutionFilesAsync(
        bool countOnly,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        EnsureStrictPushdown();

        var serializerOptions = CreateSerializerOptions();

        foreach (var filePath in _filePaths)
        {
            var fileDecisions = PartitionPruner.Evaluate(filePath, _pushdownFilter.Predicates);
            if (fileDecisions.Any(decision => !decision.MayMatch))
            {
                continue;
            }

            await using var stream = System.IO.File.OpenRead(filePath);
            using var reader = await ParquetReader.CreateAsync(stream, _parquetOptions, leaveStreamOpen: false, cancellationToken);
            var executionFilePlan = await BuildReadableExecutionFilePlanAsync(filePath, fileDecisions, reader, countOnly, cancellationToken);
            if (!executionFilePlan.FilePlan.ShouldRead || executionFilePlan.MaterializationPlan is null)
            {
                continue;
            }

            yield return new OpenQueryExecutionFile<TSource>(executionFilePlan, reader, serializerOptions);
        }
    }

    private async Task<QueryExecutionPlan<TSource>> BuildExecutionPlanAsync(CancellationToken cancellationToken, bool countOnly = false)
    {
        EnsureStrictPushdown();

        var files = new List<QueryExecutionFilePlan<TSource>>(_filePaths.Count);
        var filePlans = new List<QueryFilePlan>(_filePaths.Count);
        var readColumns = new HashSet<string>(StringComparer.Ordinal);
        var filterColumns = new HashSet<string>(StringComparer.Ordinal);
        var deferredColumns = new HashSet<string>(StringComparer.Ordinal);
        var requiresFullMaterialization = false;

        foreach (var filePath in _filePaths)
        {
            var executionFilePlan = await BuildExecutionFilePlanAsync(filePath, countOnly, cancellationToken);
            files.Add(executionFilePlan);
            filePlans.Add(executionFilePlan.FilePlan);

            if (executionFilePlan.MaterializationPlan is null)
            {
                continue;
            }

            var materializationPlan = executionFilePlan.MaterializationPlan;
            requiresFullMaterialization |= materializationPlan.RequiresFullMaterialization;
            readColumns.UnionWith(materializationPlan.RequiredColumnPaths);
            filterColumns.UnionWith(materializationPlan.FilterColumnPaths);
            deferredColumns.UnionWith(materializationPlan.DeferredColumnPaths);
        }

        var plan = new ParquetQueryPlan(
            filePlans,
            _pushdownFilter.Predicates.Select(predicate => predicate.Description).ToArray(),
            _residualPredicates.Select(diagnostic => diagnostic.ExpressionText).ToArray(),
            _residualPredicates.Select(diagnostic => diagnostic.ToString()).ToArray(),
            readColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray(),
            filterColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray(),
            deferredColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray(),
            requiresFullMaterialization);
        return new QueryExecutionPlan<TSource>(plan, files);
    }

    private async Task<QueryExecutionFilePlan<TSource>> BuildExecutionFilePlanAsync(
        string filePath,
        bool countOnly,
        CancellationToken cancellationToken)
    {
        var fileDecisions = PartitionPruner.Evaluate(filePath, _pushdownFilter.Predicates);
        if (fileDecisions.Any(decision => !decision.MayMatch))
        {
            var skippedPlan = new QueryFilePlan(
                filePath,
                shouldRead: false,
                reason: "Path partitions ruled the file out.",
                decisions: fileDecisions,
                rowGroups: Array.Empty<RowGroupPlan>(),
                pageIndexAvailable: false);
            return new QueryExecutionFilePlan<TSource>(filePath, skippedPlan, materializationPlan: null);
        }

        await using var stream = System.IO.File.OpenRead(filePath);
        using var reader = await ParquetReader.CreateAsync(stream, _parquetOptions, leaveStreamOpen: false, cancellationToken);
        return await BuildReadableExecutionFilePlanAsync(filePath, fileDecisions, reader, countOnly, cancellationToken);
    }

    private async Task<QueryExecutionFilePlan<TSource>> BuildReadableExecutionFilePlanAsync(
        string filePath,
        IReadOnlyList<FilePredicateDecision> fileDecisions,
        ParquetReader reader,
        bool countOnly,
        CancellationToken cancellationToken)
    {
        var materializationPlan = countOnly
            ? CreateCountMaterializationPlan(reader.Schema)
            : CreateMaterializationPlan(reader.Schema);
        var filePlan = await RowGroupPlanner.BuildFilePlanAsync(
            filePath,
            reader,
            _pushdownFilter,
            fileDecisions,
            _predicatePlanners,
            cancellationToken).ConfigureAwait(false);
        return new QueryExecutionFilePlan<TSource>(filePath, filePlan, materializationPlan);
    }

    private static async Task<IReadOnlyList<TResult>> ReadProjectedValuesAsync(
        ParquetReader reader,
        int rowGroupIndex,
        long rowGroupRowCount,
        ParquetSerializerOptions? serializerOptions,
        ProjectionPlan<TSource, TResult> projectionPlan,
        IReadOnlyList<RowInterval> candidateIntervals,
        CancellationToken cancellationToken)
    {
        if (projectionPlan.DirectScalarBinding?.RequiresFullRowRead == true)
        {
            var fullRows = new List<TSource>();
            await ParquetSerializer.DeserializeAsync(
                reader,
                rowGroupIndex,
                fullRows,
                cancellationToken,
                resultsAlreadyAllocated: false,
                options: serializerOptions);
            return fullRows.Select(projectionPlan.ProjectRow).ToArray();
        }

        var rowIndexes = PagePruner.ExpandRowIndexes(candidateIntervals, checked((int)rowGroupRowCount));
        if (projectionPlan.DirectScalarBinding is null || rowIndexes.Count == 0)
        {
            return Array.Empty<TResult>();
        }

        var values = await PartialRowMaterializer<TSource>.ReadColumnValuesAsync(
            reader,
            rowGroupIndex,
            projectionPlan.DirectScalarBinding.ColumnPath,
            rowIndexes,
            cancellationToken);
        return values.Select(projectionPlan.ProjectValue).ToArray();
    }

    private static async Task<IReadOnlyList<TResult>> ProjectDirectScalarResultsAsync(
        ParquetReader reader,
        int rowGroupIndex,
        ParquetSerializerOptions? serializerOptions,
        SourceMaterializationPlan<TSource> materializationPlan,
        ProjectionPlan<TSource, TResult> projectionPlan,
        MaterializedRowSet<TSource> rowSet,
        IReadOnlyList<int> selectedIndexes,
        CancellationToken cancellationToken)
    {
        var binding = projectionPlan.DirectScalarBinding
            ?? throw new InvalidOperationException("The projection plan did not contain a direct scalar binding.");
        if (binding.RequiresFullRowRead)
        {
            return await ProjectFromFullRowsAsync(rowGroupIndex, reader, serializerOptions, projectionPlan, materializationPlan, rowSet, selectedIndexes, cancellationToken);
        }

        if (materializationPlan.FilterBindings.Any(filterBinding => string.Equals(filterBinding.MemberPath, binding.MemberPath, StringComparison.Ordinal)))
        {
            return selectedIndexes
                .Select(index => projectionPlan.ProjectValue(binding.Read(rowSet.Rows[index])))
                .ToArray();
        }

        var selectedRowIndexes = selectedIndexes
            .Select(index => rowSet.RowIndexes[index])
            .ToArray();
        var values = await PartialRowMaterializer<TSource>.ReadColumnValuesAsync(
            reader,
            rowGroupIndex,
            binding.ColumnPath,
            selectedRowIndexes,
            cancellationToken);
        return values.Select(projectionPlan.ProjectValue).ToArray();
    }

    private static async Task<IReadOnlyList<TResult>> ProjectVectorizedResultsAsync(
        ParquetReader reader,
        int rowGroupIndex,
        ParquetSerializerOptions? serializerOptions,
        SourceMaterializationPlan<TSource> materializationPlan,
        ProjectionPlan<TSource, TResult> projectionPlan,
        MaterializedRowSet<TSource> rowSet,
        IReadOnlyList<int> selectedIndexes,
        CancellationToken cancellationToken)
    {
        if (projectionPlan.VectorizedBindings.Count == 0)
        {
            return Array.Empty<TResult>();
        }

        if (projectionPlan.VectorizedBindings.Any(binding => binding.RequiresFullRowRead))
        {
            return await ProjectFromFullRowsAsync(rowGroupIndex, reader, serializerOptions, projectionPlan, materializationPlan, rowSet, selectedIndexes, cancellationToken);
        }

        var selectedRowIndexes = selectedIndexes
            .Select(index => rowSet.RowIndexes[index])
            .ToArray();
        var valuesByMemberPath = new Dictionary<string, object?[]>(StringComparer.Ordinal);

        foreach (var binding in projectionPlan.VectorizedBindings)
        {
            if (materializationPlan.FilterBindings.Any(filterBinding => string.Equals(filterBinding.MemberPath, binding.MemberPath, StringComparison.Ordinal)))
            {
                var bufferedValues = new object?[selectedIndexes.Count];
                for (var index = 0; index < selectedIndexes.Count; index++)
                {
                    bufferedValues[index] = binding.Read(rowSet.Rows[selectedIndexes[index]]);
                }

                valuesByMemberPath[binding.MemberPath] = bufferedValues;
                continue;
            }

            valuesByMemberPath[binding.MemberPath] = await PartialRowMaterializer<TSource>.ReadColumnValuesAsync(
                reader,
                rowGroupIndex,
                binding.ColumnPath,
                selectedRowIndexes,
                cancellationToken);
        }

        var results = new TResult[selectedIndexes.Count];
        var rowValues = new Dictionary<string, object?>(StringComparer.Ordinal);
        for (var rowIndex = 0; rowIndex < selectedIndexes.Count; rowIndex++)
        {
            rowValues.Clear();
            foreach (var entry in valuesByMemberPath)
            {
                rowValues[entry.Key] = entry.Value[rowIndex];
            }

            results[rowIndex] = projectionPlan.ProjectVectorized(rowValues);
        }

        return results;
    }

    private static async Task<IReadOnlyList<TResult>> ProjectFromFullRowsAsync(
        int rowGroupIndex,
        ParquetReader reader,
        ParquetSerializerOptions? serializerOptions,
        ProjectionPlan<TSource, TResult> projectionPlan,
        SourceMaterializationPlan<TSource> materializationPlan,
        MaterializedRowSet<TSource> rowSet,
        IReadOnlyList<int> selectedIndexes,
        CancellationToken cancellationToken)
    {
        var fullRows = new List<TSource>();
        await ParquetSerializer.DeserializeAsync(
            reader,
            rowGroupIndex,
            fullRows,
            cancellationToken,
            resultsAlreadyAllocated: false,
            options: serializerOptions);
        foreach (var binding in materializationPlan.DeferredBindings)
        {
            for (var index = 0; index < selectedIndexes.Count; index++)
            {
                var selectedIndex = selectedIndexes[index];
                var rowIndex = rowSet.RowIndexes[selectedIndex];
                binding.Assign(rowSet.Rows[selectedIndex], binding.Read(fullRows[rowIndex]));
            }
        }

        if (projectionPlan.IsDirectScalar && projectionPlan.DirectScalarBinding is not null)
        {
            return selectedIndexes
                .Select(index => projectionPlan.ProjectValue(projectionPlan.DirectScalarBinding.Read(rowSet.Rows[index])))
                .ToArray();
        }

        return selectedIndexes
            .Select(index => projectionPlan.ProjectRow(rowSet.Rows[index]))
            .ToArray();
    }
}
