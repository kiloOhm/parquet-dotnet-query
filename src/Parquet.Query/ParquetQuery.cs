using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using Parquet;
using Parquet.Query.Dynamic;
using Parquet.Query.Expressions;
using Parquet.Query.Internal;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;
using Parquet.Serialization;

namespace Parquet.Query;

/// <summary>
/// Creates <see cref="ParquetQuery{TSource, TResult}"/> pipelines from files or directories.
/// </summary>
public static class ParquetQuery
{
    /// <summary>
    /// Creates a query over a single parquet file.
    /// </summary>
    /// <typeparam name="T">The row type to deserialize from the file.</typeparam>
    /// <param name="filePath">The parquet file to query.</param>
    /// <param name="parquetOptions">Optional reader options to apply.</param>
    /// <returns>A query that yields rows of <typeparamref name="T"/>.</returns>
    public static ParquetQuery<T, T> FromFile<T>(string filePath, ParquetOptions? parquetOptions = null)
        where T : class, new()
        => ParquetQuery<T, T>.FromFiles(new[] { filePath }, parquetOptions);

    /// <summary>
    /// Creates a query over a set of parquet files.
    /// </summary>
    /// <typeparam name="T">The row type to deserialize from each file.</typeparam>
    /// <param name="filePaths">The parquet files to query.</param>
    /// <param name="parquetOptions">Optional reader options to apply.</param>
    /// <returns>A query that yields rows of <typeparamref name="T"/>.</returns>
    public static ParquetQuery<T, T> FromFiles<T>(IEnumerable<string> filePaths, ParquetOptions? parquetOptions = null)
        where T : class, new()
        => ParquetQuery<T, T>.FromFiles(filePaths, parquetOptions);

    /// <summary>
    /// Creates a query over all parquet files that match a search within a directory.
    /// </summary>
    /// <typeparam name="T">The row type to deserialize from each file.</typeparam>
    /// <param name="directoryPath">The root directory that contains parquet files.</param>
    /// <param name="searchPattern">The file search pattern to use.</param>
    /// <param name="searchOption">Whether to search only the directory or all subdirectories.</param>
    /// <param name="parquetOptions">Optional reader options to apply.</param>
    /// <returns>A query that yields rows of <typeparamref name="T"/>.</returns>
    public static ParquetQuery<T, T> FromDirectory<T>(
        string directoryPath,
        string searchPattern = "*.parquet",
        SearchOption searchOption = SearchOption.AllDirectories,
        ParquetOptions? parquetOptions = null)
        where T : class, new()
        => ParquetQuery<T, T>.FromFiles(Directory.EnumerateFiles(directoryPath, searchPattern, searchOption), parquetOptions);

    /// <summary>
    /// Creates a dynamic (non-generic) query over an already-open parquet reader.
    /// Use this when the row type is not known at compile time.
    /// </summary>
    /// <param name="reader">An open parquet reader. The caller retains ownership.</param>
    /// <param name="filePath">The file path associated with the reader.</param>
    /// <returns>A dynamic query with no predicates.</returns>
    public static DynamicParquetQuery FromReader(ParquetReader reader, string? filePath = null)
        => DynamicParquetQuery.FromReader(reader, filePath);
}

/// <summary>
/// Represents an immutable parquet query pipeline that can be planned, explained, or executed.
/// </summary>
/// <typeparam name="TSource">The source row type read from parquet files.</typeparam>
/// <typeparam name="TResult">The result type produced by the query projection.</typeparam>
public sealed class ParquetQuery<TSource, TResult>
    where TSource : class, new()
{
    private static readonly StringComparer FilePathComparer = PlatformCompatibility.IsWindows()
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
    private readonly IParquetReaderFactory _readerFactory;
    private readonly IParquetQueryCache _queryCache;
    private readonly bool _strictPushdown;

    private ParquetQuery(
        IReadOnlyList<string> filePaths,
        ParquetOptions? parquetOptions,
        PushdownFilter<TSource> pushdownFilter,
        IReadOnlyList<IParquetPredicatePlanner<TSource>> predicatePlanners,
        IReadOnlyList<Expression<Func<TSource, bool>>> wherePredicates,
        IReadOnlyList<PredicatePushdownDiagnostic> residualPredicates,
        Expression<Func<TSource, TResult>>? projection,
        IParquetReaderFactory readerFactory,
        IParquetQueryCache queryCache,
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
        _readerFactory = readerFactory ?? throw new ArgumentNullException(nameof(readerFactory));
        _queryCache = queryCache ?? throw new ArgumentNullException(nameof(queryCache));
        _strictPushdown = strictPushdown;
    }

    /// <summary>
    /// Creates a query over a set of parquet files.
    /// </summary>
    /// <param name="filePaths">The parquet files to query.</param>
    /// <param name="parquetOptions">Optional reader options to apply.</param>
    /// <returns>A new query rooted at the provided files.</returns>
    public static ParquetQuery<TSource, TResult> FromFiles(IEnumerable<string> filePaths, ParquetOptions? parquetOptions = null)
    {
        Guard.NotNull(filePaths, nameof(filePaths));

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
            FileParquetReaderFactory.Instance,
            DefaultParquetQueryCache.Instance,
            strictPushdown: false);
    }

    /// <summary>
    /// Adds explicit pushdown predicates to the query.
    /// </summary>
    /// <param name="filter">The filter to combine with any existing pushdown filter.</param>
    /// <returns>A new query with the combined pushdown filter.</returns>
    public ParquetQuery<TSource, TResult> Pushdown(PushdownFilter<TSource> filter)
    {
        Guard.NotNull(filter, nameof(filter));

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter.And(filter),
            _predicatePlanners,
            _wherePredicates,
            _residualPredicates,
            _projection,
            _readerFactory,
            _queryCache,
            _strictPushdown);
    }

    /// <summary>
    /// Builds and applies a pushdown filter by using a fluent builder.
    /// </summary>
    /// <param name="configure">A callback that configures the pushdown filter builder.</param>
    /// <returns>A new query with the configured pushdown filter.</returns>
    public ParquetQuery<TSource, TResult> Pushdown(Func<PushdownFilterBuilder<TSource>, PushdownFilterBuilder<TSource>> configure) =>
        Pushdown(global::Parquet.Query.Pushdown.Pushdown.For(configure));

    /// <summary>
    /// Adds a LINQ predicate to the query and extracts any pushdown-eligible fragments from it.
    /// </summary>
    /// <param name="predicate">The predicate to apply.</param>
    /// <returns>A new query with the predicate appended.</returns>
    public ParquetQuery<TSource, TResult> Where(Expression<Func<TSource, bool>> predicate)
    {
        Guard.NotNull(predicate, nameof(predicate));

        var split = PredicatePushdownExtractor.Extract(predicate);

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter.And(split.PushdownFilter),
            _predicatePlanners,
            new ReadOnlyCollection<Expression<Func<TSource, bool>>>(_wherePredicates.Concat(new[] { predicate }).ToArray()),
            new ReadOnlyCollection<PredicatePushdownDiagnostic>(_residualPredicates.Concat(split.Diagnostics).ToArray()),
            _projection,
            _readerFactory,
            _queryCache,
            _strictPushdown);
    }

    /// <summary>
    /// Projects each matching source row into a new result shape.
    /// </summary>
    /// <typeparam name="TNextResult">The projected result type.</typeparam>
    /// <param name="projection">The projection expression.</param>
    /// <returns>A new query that yields projected values.</returns>
    public ParquetQuery<TSource, TNextResult> Select<TNextResult>(Expression<Func<TSource, TNextResult>> projection)
    {
        Guard.NotNull(projection, nameof(projection));

        return new ParquetQuery<TSource, TNextResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter,
            _predicatePlanners,
            _wherePredicates,
            _residualPredicates,
            projection,
            _readerFactory,
            _queryCache,
            _strictPushdown);
    }

    /// <summary>
    /// Controls whether non-pushdownable predicates should cause execution to fail.
    /// </summary>
    /// <param name="enabled"><see langword="true"/> to throw when residual predicates remain; otherwise <see langword="false"/>.</param>
    /// <returns>A new query with the strict pushdown setting applied.</returns>
    public ParquetQuery<TSource, TResult> StrictPushdown(bool enabled = true) =>
        new(
            _filePaths,
            _parquetOptions,
            _pushdownFilter,
            _predicatePlanners,
            _wherePredicates,
            _residualPredicates,
            _projection,
            _readerFactory,
            _queryCache,
            enabled);

    /// <summary>
    /// Adds a predicate planner that can prune row groups or pages for custom predicates.
    /// </summary>
    /// <param name="predicatePlanner">The planner to add.</param>
    /// <returns>A new query with the planner registered.</returns>
    public ParquetQuery<TSource, TResult> WithPredicatePlanner(IParquetPredicatePlanner<TSource> predicatePlanner)
    {
        Guard.NotNull(predicatePlanner, nameof(predicatePlanner));
        return WithPredicatePlanners(new[] { predicatePlanner });
    }

    /// <summary>
    /// Adds predicate planners that can prune row groups or pages for custom predicates.
    /// </summary>
    /// <param name="predicatePlanners">The planners to add.</param>
    /// <returns>A new query with the planners registered.</returns>
    public ParquetQuery<TSource, TResult> WithPredicatePlanners(IEnumerable<IParquetPredicatePlanner<TSource>> predicatePlanners)
    {
        Guard.NotNull(predicatePlanners, nameof(predicatePlanners));

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
            _readerFactory,
            _queryCache,
            _strictPushdown);
    }

    /// <summary>
    /// Replaces the parquet reader options used by this query.
    /// </summary>
    /// <param name="parquetOptions">The options to clone into the query.</param>
    /// <returns>A new query with the supplied parquet options.</returns>
    public ParquetQuery<TSource, TResult> WithParquetOptions(ParquetOptions parquetOptions)
    {
        Guard.NotNull(parquetOptions, nameof(parquetOptions));

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            ParquetOptionsFactory.Clone(parquetOptions),
            _pushdownFilter,
            _predicatePlanners,
            _wherePredicates,
            _residualPredicates,
            _projection,
            _readerFactory,
            _queryCache,
            _strictPushdown);
    }

    /// <summary>
    /// Clones the current parquet options, applies a configuration callback, and returns a new query.
    /// </summary>
    /// <param name="configure">The callback that mutates the cloned options.</param>
    /// <returns>A new query with the updated parquet options.</returns>
    public ParquetQuery<TSource, TResult> ConfigureParquetOptions(Action<ParquetOptions> configure)
    {
        Guard.NotNull(configure, nameof(configure));

        var options = ParquetOptionsFactory.Clone(_parquetOptions);
        configure(options);
        return WithParquetOptions(options);
    }

    /// <summary>
    /// Routes parquet reader creation through a custom reader factory.
    /// </summary>
    public ParquetQuery<TSource, TResult> WithReaderFactory(IParquetReaderFactory readerFactory)
    {
        Guard.NotNull(readerFactory, nameof(readerFactory));

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter,
            _predicatePlanners,
            _wherePredicates,
            _residualPredicates,
            _projection,
            readerFactory,
            _queryCache,
            _strictPushdown);
    }

    /// <summary>
    /// Routes query planning through a custom cache.
    /// </summary>
    public ParquetQuery<TSource, TResult> WithQueryCache(IParquetQueryCache queryCache)
    {
        Guard.NotNull(queryCache, nameof(queryCache));

        return new ParquetQuery<TSource, TResult>(
            _filePaths,
            _parquetOptions,
            _pushdownFilter,
            _predicatePlanners,
            _wherePredicates,
            _residualPredicates,
            _projection,
            _readerFactory,
            queryCache,
            _strictPushdown);
    }

    /// <summary>
    /// Disables query-plan caching for this query pipeline.
    /// </summary>
    public ParquetQuery<TSource, TResult> WithoutQueryCache() =>
        WithQueryCache(NullParquetQueryCache.Instance);

    /// <summary>
    /// Configures footer encryption for all queried files.
    /// </summary>
    /// <param name="footerEncryptionKey">The encryption key to use for footer metadata.</param>
    /// <param name="keyMetadata">Optional key metadata associated with the key.</param>
    /// <returns>A new query with footer encryption options applied.</returns>
    public ParquetQuery<TSource, TResult> WithFooterKey(string footerEncryptionKey, byte[]? keyMetadata = null) =>
        ConfigureParquetOptions(options =>
        {
            options.FooterEncryptionKey = footerEncryptionKey;
            options.FooterEncryptionKeyMetadata = keyMetadata?.ToArray();
        });

    /// <summary>
    /// Configures footer signing for all queried files.
    /// </summary>
    /// <param name="footerSigningKey">The signing key to use for footer metadata.</param>
    /// <param name="keyMetadata">Optional key metadata associated with the key.</param>
    /// <returns>A new query with footer signing options applied.</returns>
    public ParquetQuery<TSource, TResult> WithFooterSigningKey(string footerSigningKey, byte[]? keyMetadata = null) =>
        ConfigureParquetOptions(options =>
        {
            options.FooterSigningKey = footerSigningKey;
            options.FooterSigningKeyMetadata = keyMetadata?.ToArray();
        });

    /// <summary>
    /// Configures whether the query should expect plaintext parquet footers.
    /// </summary>
    /// <param name="enabled"><see langword="true"/> to use plaintext footers; otherwise <see langword="false"/>.</param>
    /// <returns>A new query with the footer mode applied.</returns>
    public ParquetQuery<TSource, TResult> UsePlaintextFooter(bool enabled = true) =>
        ConfigureParquetOptions(options => options.UsePlaintextFooter = enabled);

    /// <summary>
    /// Configures the AAD prefix used for encrypted parquet files.
    /// </summary>
    /// <param name="aadPrefix">The AAD prefix to apply.</param>
    /// <param name="supplyOutOfBand"><see langword="true"/> to indicate the prefix is supplied out of band.</param>
    /// <returns>A new query with the AAD settings applied.</returns>
    public ParquetQuery<TSource, TResult> WithAadPrefix(string aadPrefix, bool supplyOutOfBand = false) =>
        ConfigureParquetOptions(options =>
        {
            options.AADPrefix = aadPrefix;
            options.SupplyAadPrefix = supplyOutOfBand;
        });

    /// <summary>
    /// Configures whether AES CTR mode variants should be used for parquet encryption.
    /// </summary>
    /// <param name="enabled"><see langword="true"/> to enable CTR variants; otherwise <see langword="false"/>.</param>
    /// <returns>A new query with the encryption setting applied.</returns>
    public ParquetQuery<TSource, TResult> UseCtrVariant(bool enabled = true) =>
        ConfigureParquetOptions(options => options.UseCtrVariant = enabled);

    /// <summary>
    /// Configures how column encryption keys are resolved.
    /// </summary>
    /// <param name="resolver">A callback that resolves keys for a column path and optional key metadata.</param>
    /// <returns>A new query with the resolver applied.</returns>
    public ParquetQuery<TSource, TResult> WithColumnKeyResolver(Func<IReadOnlyList<string>, byte[]?, string?> resolver) =>
        ConfigureParquetOptions(options => options.ColumnKeyResolver = resolver);

    /// <summary>
    /// Builds a plan that describes which files, row groups, pages, and columns the query will read.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel planning.</param>
    /// <returns>The computed query plan.</returns>
    public async Task<ParquetQueryPlan> PlanAsync(CancellationToken cancellationToken = default)
    {
        var executionPlan = await GetExecutionPlanAsync(cancellationToken);
        return executionPlan.QueryPlan;
    }

    /// <summary>
    /// Produces a human-readable explanation of the current query plan.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel planning.</param>
    /// <returns>A textual explanation of the query.</returns>
    public async Task<string> ExplainAsync(CancellationToken cancellationToken = default)
    {
        var executionPlan = await GetExecutionPlanAsync(cancellationToken);
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

    /// <summary>
    /// Counts the rows that match the current query.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel execution.</param>
    /// <returns>The number of matching rows.</returns>
    public async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        if (_pushdownFilter.IsEmpty && _wherePredicates.Count == 0)
        {
            var executionPlan = await GetExecutionPlanAsync(cancellationToken, countOnly: true);
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

    /// <summary>
    /// Counts the rows that match the current query.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel execution.</param>
    /// <returns>The number of matching rows.</returns>
    public Task<long> LongCountAsync(CancellationToken cancellationToken = default) =>
        CountAsync(cancellationToken);

    /// <summary>
    /// Determines whether the query returns at least one matching row.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel execution.</param>
    /// <returns><see langword="true"/> when a matching row exists; otherwise <see langword="false"/>.</returns>
    public async Task<bool> AnyAsync(CancellationToken cancellationToken = default)
    {
        if (_pushdownFilter.IsEmpty && _wherePredicates.Count == 0)
        {
            var executionPlan = await GetExecutionPlanAsync(cancellationToken, countOnly: true);
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

    /// <summary>
    /// Returns the first matching result or the default value when the query is empty.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel execution.</param>
    /// <returns>The first matching result, or the default value of <typeparamref name="TResult"/>.</returns>
    public async Task<TResult?> FirstOrDefaultAsync(CancellationToken cancellationToken = default)
    {
        await foreach (var result in EnumerateResultsAsync(cancellationToken))
        {
            return result;
        }

        return default;
    }

    /// <summary>
    /// Materializes the query into an in-memory list.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel execution.</param>
    /// <returns>A list containing all matching results.</returns>
    public async Task<IReadOnlyList<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var results = new List<TResult>();
        await foreach (var result in EnumerateResultsAsync(cancellationToken))
        {
            results.Add(result);
        }

        return results;
    }

    /// <summary>
    /// Streams the query results asynchronously.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel execution.</param>
    /// <returns>An asynchronous sequence of query results.</returns>
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
        var serializerOptions = CreateSerializerOptions();
        var executionPlan = await GetExecutionPlanAsync(cancellationToken, countOnly).ConfigureAwait(false);

        foreach (var executionFilePlan in executionPlan.Files)
        {
            if (!executionFilePlan.FilePlan.ShouldRead || executionFilePlan.MaterializationPlan is null)
            {
                continue;
            }

            await using var readerLease = await _readerFactory.RentAsync(executionFilePlan.FilePath, _parquetOptions, cancellationToken).ConfigureAwait(false);
            var reader = readerLease.Reader;
            yield return new OpenQueryExecutionFile<TSource>(executionFilePlan, reader, serializerOptions);
        }
    }

    private async Task<QueryExecutionPlan<TSource>> GetExecutionPlanAsync(CancellationToken cancellationToken, bool countOnly = false)
    {
        EnsureStrictPushdown();

        var cacheKey = ParquetQueryCacheKeyBuilder.Build(
            _filePaths,
            _parquetOptions,
            _pushdownFilter,
            _predicatePlanners,
            _wherePredicates,
            _projection,
            _readerFactory,
            countOnly);

        var cached = await _queryCache.GetAsync(cacheKey, cancellationToken).ConfigureAwait(false);
        if (cached is QueryExecutionPlan<TSource> executionPlan)
        {
            return executionPlan;
        }

        var builtPlan = await BuildExecutionPlanCoreAsync(cancellationToken, countOnly).ConfigureAwait(false);
        await _queryCache.SetAsync(cacheKey, builtPlan, cancellationToken).ConfigureAwait(false);
        return builtPlan;
    }

    private async Task<QueryExecutionPlan<TSource>> BuildExecutionPlanCoreAsync(CancellationToken cancellationToken, bool countOnly = false)
    {
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

        await using var readerLease = await _readerFactory.RentAsync(filePath, _parquetOptions, cancellationToken).ConfigureAwait(false);
        var reader = readerLease.Reader;
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
