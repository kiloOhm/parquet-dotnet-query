using System.Runtime.CompilerServices;
using Parquet;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;
using Parquet.Schema;

namespace Parquet.Query.Dynamic;

/// <summary>
/// Provides a non-generic query entry point for parquet files where the row type
/// is not known at compile time. Predicates are expressed as column/operator/value
/// strings and flow through the full pushdown planning pipeline (statistics, bloom
/// filters, page indexes, and custom index planners).
/// </summary>
public sealed class DynamicParquetQuery
{
    private readonly string _filePath;
    private readonly ParquetReader _reader;
    private readonly IReadOnlyList<DynamicPredicate> _predicates;
    private readonly IReadOnlyList<IParquetPredicatePlanner<DynamicRow>> _predicatePlanners;

    private DynamicParquetQuery(
        string filePath,
        ParquetReader reader,
        IReadOnlyList<DynamicPredicate> predicates,
        IReadOnlyList<IParquetPredicatePlanner<DynamicRow>> predicatePlanners)
    {
        _filePath = filePath;
        _reader = reader;
        _predicates = predicates;
        _predicatePlanners = predicatePlanners;
    }

    private static readonly IReadOnlyList<IParquetPredicatePlanner<DynamicRow>> DefaultPlanners =
        new[] { DynamicFooterIndexPlanner.Instance };

    /// <summary>
    /// Creates a dynamic query over an already-open parquet reader. The caller retains
    /// ownership of the reader and is responsible for disposing it.
    /// </summary>
    /// <param name="reader">An open parquet reader.</param>
    /// <param name="filePath">The file path associated with the reader (used in plan descriptions).</param>
    /// <returns>A new dynamic query with no predicates.</returns>
    public static DynamicParquetQuery FromReader(ParquetReader reader, string? filePath = null)
    {
        if (reader is null) throw new ArgumentNullException(nameof(reader));

        return new DynamicParquetQuery(
            filePath ?? string.Empty,
            reader,
            Array.Empty<DynamicPredicate>(),
            DefaultPlanners);
    }

    /// <summary>
    /// Adds predicates to the query.
    /// </summary>
    /// <param name="predicates">The predicates to add.</param>
    /// <returns>A new query with the additional predicates.</returns>
    public DynamicParquetQuery Where(params DynamicPredicate[] predicates)
    {
        if (predicates is null || predicates.Length == 0) return this;

        return new DynamicParquetQuery(
            _filePath,
            _reader,
            _predicates.Concat(predicates).ToArray(),
            _predicatePlanners);
    }

    /// <summary>
    /// Registers custom predicate planners that can prune row groups or pages using
    /// footer-embedded indices or other custom metadata.
    /// </summary>
    /// <param name="planners">The planners to register.</param>
    /// <returns>A new query with the additional planners.</returns>
    internal DynamicParquetQuery WithPredicatePlanners(IEnumerable<IParquetPredicatePlanner<DynamicRow>> planners)
    {
        if (planners is null) return this;

        return new DynamicParquetQuery(
            _filePath,
            _reader,
            _predicates,
            _predicatePlanners.Concat(planners).ToArray());
    }

    /// <summary>
    /// Builds a plan that describes which row groups and pages the query will read or skip.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel planning.</param>
    /// <returns>The computed query plan.</returns>
    public async Task<ParquetQueryPlan> PlanAsync(CancellationToken cancellationToken = default)
    {
        var dataFields = GetDataFields();
        var filterPlan = DynamicPredicateCompiler.Compile(_predicates, dataFields);

        var filePlan = await RowGroupPlanner.BuildFilePlanAsync<DynamicRow>(
            _filePath,
            _reader,
            filterPlan.PushdownFilter,
            Array.Empty<FilePredicateDecision>(),
            _predicatePlanners,
            cancellationToken).ConfigureAwait(false);

        return BuildQueryPlan(filePlan, filterPlan, dataFields);
    }

    /// <summary>
    /// Executes the query and returns the matching rows for the requested page.
    /// </summary>
    /// <param name="offset">The zero-based offset into the full result set.</param>
    /// <param name="limit">The maximum number of rows to return.</param>
    /// <param name="cancellationToken">A token used to cancel execution.</param>
    /// <returns>The query result including the plan, matching rows, and total count.</returns>
    public async Task<DynamicQueryResult> ExecuteAsync(
        int offset = 0,
        int limit = 500,
        CancellationToken cancellationToken = default)
    {
        var dataFields = GetDataFields();
        var fields = dataFields.Values.ToArray();
        var filterPlan = DynamicPredicateCompiler.Compile(_predicates, dataFields);

        var filePlan = await RowGroupPlanner.BuildFilePlanAsync<DynamicRow>(
            _filePath,
            _reader,
            filterPlan.PushdownFilter,
            Array.Empty<FilePredicateDecision>(),
            _predicatePlanners,
            cancellationToken).ConfigureAwait(false);

        var page = new List<IReadOnlyDictionary<string, object?>>();
        long totalMatched = 0;
        var hasFilter = !filterPlan.PushdownFilter.IsEmpty || filterPlan.ResidualPredicates.Count > 0;

        foreach (var rg in filePlan.RowGroups)
        {
            if (!rg.ShouldRead || rg.CandidateRowCountUpperBound == 0)
                continue;

            var rows = await DynamicRowMaterializer.ReadRowGroupAsync(
                _reader, rg.Index, fields, rg.CandidateIntervals, cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < rows.Count; i++)
            {
                var row = rows[i];
                if (hasFilter && (!filterPlan.PushdownFilter.Matches(row) || !filterPlan.MatchesResidual(row)))
                    continue;

                if (totalMatched >= offset && page.Count < limit)
                    page.Add(row.Values);

                totalMatched++;
            }
        }

        var plan = BuildQueryPlan(filePlan, filterPlan, dataFields);
        return new DynamicQueryResult(plan, page, offset, limit, totalMatched);
    }

    /// <summary>
    /// Streams all matching rows as an asynchronous sequence of dictionaries.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel enumeration.</param>
    /// <returns>An asynchronous sequence of matching rows.</returns>
    public async IAsyncEnumerable<IReadOnlyDictionary<string, object?>> ToAsyncEnumerable(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var dataFields = GetDataFields();
        var fields = dataFields.Values.ToArray();
        var filterPlan = DynamicPredicateCompiler.Compile(_predicates, dataFields);

        var filePlan = await RowGroupPlanner.BuildFilePlanAsync<DynamicRow>(
            _filePath,
            _reader,
            filterPlan.PushdownFilter,
            Array.Empty<FilePredicateDecision>(),
            _predicatePlanners,
            cancellationToken).ConfigureAwait(false);

        var hasFilter = !filterPlan.PushdownFilter.IsEmpty || filterPlan.ResidualPredicates.Count > 0;

        foreach (var rg in filePlan.RowGroups)
        {
            if (!rg.ShouldRead || rg.CandidateRowCountUpperBound == 0)
                continue;

            var rows = await DynamicRowMaterializer.ReadRowGroupAsync(
                _reader, rg.Index, fields, rg.CandidateIntervals, cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < rows.Count; i++)
            {
                var row = rows[i];
                if (!hasFilter || (filterPlan.PushdownFilter.Matches(row) && filterPlan.MatchesResidual(row)))
                    yield return row.Values;
            }
        }
    }

    /// <summary>
    /// Counts the rows that match the current predicates.
    /// </summary>
    /// <param name="cancellationToken">A token used to cancel execution.</param>
    /// <returns>The number of matching rows.</returns>
    public async Task<long> CountAsync(CancellationToken cancellationToken = default)
    {
        if (_predicates.Count == 0)
        {
            return Enumerable.Range(0, _reader.RowGroupCount)
                .Sum(i =>
                {
                    using var rg = _reader.OpenRowGroupReader(i);
                    return rg.RowCount;
                });
        }

        var dataFields = GetDataFields();
        var fields = dataFields.Values.ToArray();
        var filterPlan = DynamicPredicateCompiler.Compile(_predicates, dataFields);

        var filePlan = await RowGroupPlanner.BuildFilePlanAsync<DynamicRow>(
            _filePath,
            _reader,
            filterPlan.PushdownFilter,
            Array.Empty<FilePredicateDecision>(),
            _predicatePlanners,
            cancellationToken).ConfigureAwait(false);

        var hasFilter = !filterPlan.PushdownFilter.IsEmpty || filterPlan.ResidualPredicates.Count > 0;
        long count = 0;

        foreach (var rg in filePlan.RowGroups)
        {
            if (!rg.ShouldRead || rg.CandidateRowCountUpperBound == 0)
                continue;

            if (!hasFilter)
            {
                count += rg.RowCount;
                continue;
            }

            var rows = await DynamicRowMaterializer.ReadRowGroupAsync(
                _reader, rg.Index, fields, rg.CandidateIntervals, cancellationToken).ConfigureAwait(false);

            for (var i = 0; i < rows.Count; i++)
            {
                if (filterPlan.PushdownFilter.Matches(rows[i]) && filterPlan.MatchesResidual(rows[i]))
                    count++;
            }
        }

        return count;
    }

    private IReadOnlyDictionary<string, DataField> GetDataFields()
    {
        return _reader.Schema.GetDataFields()
            .ToDictionary(f => f.Path.ToString(), StringComparer.Ordinal);
    }

    private ParquetQueryPlan BuildQueryPlan(
        QueryFilePlan filePlan,
        DynamicFilterPlan filterPlan,
        IReadOnlyDictionary<string, DataField> dataFields)
    {
        var pushdownDescriptions = filterPlan.PushdownFilter.Predicates
            .Select(p => p.Description).ToArray();

        var residualDescriptions = _predicates
            .Where(p => IsResidualOnly(p.Operator))
            .Select(p => p.Value2 is not null
                ? $"{p.Column} {p.Operator} {p.Value} {p.Value2}"
                : p.Value is not null
                    ? $"{p.Column} {p.Operator} {p.Value}"
                    : $"{p.Column} {p.Operator}")
            .ToArray();

        var allColumns = dataFields.Keys.ToArray();
        var filterColumns = filterPlan.PushdownFilter.Predicates
            .Select(p => p.ColumnPath)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        return new ParquetQueryPlan(
            new[] { filePlan },
            pushdownDescriptions,
            residualDescriptions,
            Array.Empty<string>(),
            allColumns,
            filterColumns,
            Array.Empty<string>(),
            requiresFullMaterialization: true);
    }

    private static bool IsResidualOnly(string op)
    {
        var normalized = op.Trim().ToLowerInvariant();
        return normalized == "endswith" ||
               normalized == "contains" ||
               normalized == "isnull" ||
               normalized == "isnotnull";
    }
}
