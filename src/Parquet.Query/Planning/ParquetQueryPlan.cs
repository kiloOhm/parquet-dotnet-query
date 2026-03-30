namespace Parquet.Query.Planning;

/// <summary>
/// Describes how a parquet query will be executed across files, row groups, pages, and columns.
/// </summary>
public sealed class ParquetQueryPlan
{
    private readonly IReadOnlyList<RowGroupPlan> _rowGroups;

    /// <summary>
    /// Initializes a new query plan.
    /// </summary>
    /// <param name="files">The per-file execution plans.</param>
    /// <param name="pushdownPredicates">The predicates applied during pushdown planning.</param>
    /// <param name="residualPredicates">The predicates that still require row evaluation.</param>
    /// <param name="residualPredicateDiagnostics">Diagnostics for residual predicates.</param>
    /// <param name="readColumns">The columns that will be read.</param>
    /// <param name="filterColumns">The columns needed to evaluate filters.</param>
    /// <param name="deferredColumns">The columns read only after filtering.</param>
    /// <param name="requiresFullMaterialization">Whether the query must materialize full rows.</param>
    public ParquetQueryPlan(
        IReadOnlyList<QueryFilePlan> files,
        IReadOnlyList<string> pushdownPredicates,
        IReadOnlyList<string> residualPredicates,
        IReadOnlyList<string> residualPredicateDiagnostics,
        IReadOnlyList<string> readColumns,
        IReadOnlyList<string> filterColumns,
        IReadOnlyList<string> deferredColumns,
        bool requiresFullMaterialization)
    {
        Files = files;
        PushdownPredicates = pushdownPredicates;
        ResidualPredicates = residualPredicates;
        ResidualPredicateDiagnostics = residualPredicateDiagnostics;
        ReadColumns = readColumns;
        FilterColumns = filterColumns;
        DeferredColumns = deferredColumns;
        RequiresFullMaterialization = requiresFullMaterialization;
        _rowGroups = files.SelectMany(file => file.RowGroups).ToArray();
    }

    /// <summary>
    /// Gets the single file path when the plan contains exactly one file; otherwise an empty string.
    /// </summary>
    public string FilePath => Files.Count == 1 ? Files[0].FilePath : string.Empty;

    /// <summary>
    /// Gets the per-file execution plans.
    /// </summary>
    public IReadOnlyList<QueryFilePlan> Files { get; }

    /// <summary>
    /// Gets the predicates applied during pushdown planning.
    /// </summary>
    public IReadOnlyList<string> PushdownPredicates { get; }

    /// <summary>
    /// Gets the predicates that still require row evaluation.
    /// </summary>
    public IReadOnlyList<string> ResidualPredicates { get; }

    /// <summary>
    /// Gets diagnostics explaining why some predicates remained residual.
    /// </summary>
    public IReadOnlyList<string> ResidualPredicateDiagnostics { get; }

    /// <summary>
    /// Gets the columns that will be read from parquet files.
    /// </summary>
    public IReadOnlyList<string> ReadColumns { get; }

    /// <summary>
    /// Gets the columns needed to evaluate filters.
    /// </summary>
    public IReadOnlyList<string> FilterColumns { get; }

    /// <summary>
    /// Gets the columns that are deferred until after filtering.
    /// </summary>
    public IReadOnlyList<string> DeferredColumns { get; }

    /// <summary>
    /// Gets a value indicating whether the query must materialize full source rows.
    /// </summary>
    public bool RequiresFullMaterialization { get; }

    /// <summary>
    /// Gets a value indicating whether the plan uses late materialization.
    /// </summary>
    public bool UsesLateMaterialization => !RequiresFullMaterialization && DeferredColumns.Count > 0;

    /// <summary>
    /// Gets a value indicating whether any file exposes persisted page indexes.
    /// </summary>
    public bool PageIndexAvailable => Files.Any(file => file.PageIndexAvailable);

    /// <summary>
    /// Gets a value indicating whether any row group fell back to derived page metadata.
    /// </summary>
    public bool UsedFallbackPageIndex => RowGroups.Any(rowGroup => rowGroup.UsedFallbackPageIndex);

    /// <summary>
    /// Gets the row-group plans across all files.
    /// </summary>
    public IReadOnlyList<RowGroupPlan> RowGroups => _rowGroups;

    /// <summary>
    /// Gets the number of files that will be read.
    /// </summary>
    public int SelectedFileCount => Files.Count(file => file.ShouldRead);

    /// <summary>
    /// Gets the number of row groups that will be read.
    /// </summary>
    public int SelectedRowGroupCount => RowGroups.Count(rowGroup => rowGroup.ShouldRead);

    /// <summary>
    /// Gets an upper bound on the total number of candidate rows that remain after pruning.
    /// </summary>
    public long SelectedRowCountUpperBound => RowGroups.Where(rowGroup => rowGroup.ShouldRead).Sum(rowGroup => rowGroup.CandidateRowCountUpperBound);

    /// <summary>
    /// Gets the total number of selected pages across all readable row groups.
    /// </summary>
    public int SelectedPageCount => RowGroups.Where(rowGroup => rowGroup.ShouldRead).Sum(rowGroup => rowGroup.SelectedPageCount);

    /// <summary>
    /// Gets the total number of pages observed across all row groups.
    /// </summary>
    public int TotalPageCount => RowGroups.Sum(rowGroup => rowGroup.PageCount);
}

/// <summary>
/// Describes how a single file participates in a query plan.
/// </summary>
public sealed class QueryFilePlan
{
    /// <summary>
    /// Initializes a new file plan.
    /// </summary>
    /// <param name="filePath">The parquet file path.</param>
    /// <param name="shouldRead">Whether the file will be read.</param>
    /// <param name="reason">The reason the file is read or skipped.</param>
    /// <param name="decisions">The partition pruning decisions for the file.</param>
    /// <param name="rowGroups">The row-group plans within the file.</param>
    /// <param name="pageIndexAvailable">Whether the file exposes page indexes.</param>
    public QueryFilePlan(
        string filePath,
        bool shouldRead,
        string reason,
        IReadOnlyList<FilePredicateDecision> decisions,
        IReadOnlyList<RowGroupPlan> rowGroups,
        bool pageIndexAvailable)
    {
        FilePath = filePath;
        ShouldRead = shouldRead;
        Reason = reason;
        Decisions = decisions;
        RowGroups = rowGroups;
        PageIndexAvailable = pageIndexAvailable;
    }

    /// <summary>
    /// Gets the parquet file path.
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Gets a value indicating whether the file will be read.
    /// </summary>
    public bool ShouldRead { get; }

    /// <summary>
    /// Gets the reason the file is read or skipped.
    /// </summary>
    public string Reason { get; }

    /// <summary>
    /// Gets the partition pruning decisions for the file.
    /// </summary>
    public IReadOnlyList<FilePredicateDecision> Decisions { get; }

    /// <summary>
    /// Gets the row-group plans within the file.
    /// </summary>
    public IReadOnlyList<RowGroupPlan> RowGroups { get; }

    /// <summary>
    /// Gets a value indicating whether the file exposes page indexes.
    /// </summary>
    public bool PageIndexAvailable { get; }

    /// <summary>
    /// Gets the number of row groups that will be read from the file.
    /// </summary>
    public int SelectedRowGroupCount => RowGroups.Count(rowGroup => rowGroup.ShouldRead);
}

/// <summary>
/// Describes the result of evaluating a partition predicate against a file path.
/// </summary>
public sealed class FilePredicateDecision
{
    /// <summary>
    /// Initializes a new file predicate decision.
    /// </summary>
    /// <param name="predicate">The predicate description.</param>
    /// <param name="mayMatch">Whether the file may match the predicate.</param>
    /// <param name="source">The component that made the decision.</param>
    /// <param name="reason">A human-readable explanation.</param>
    public FilePredicateDecision(string predicate, bool mayMatch, string source, string reason)
    {
        Predicate = predicate;
        MayMatch = mayMatch;
        Source = source;
        Reason = reason;
    }

    /// <summary>
    /// Gets the predicate description.
    /// </summary>
    public string Predicate { get; }

    /// <summary>
    /// Gets a value indicating whether the file may match the predicate.
    /// </summary>
    public bool MayMatch { get; }

    /// <summary>
    /// Gets the component that made the decision.
    /// </summary>
    public string Source { get; }

    /// <summary>
    /// Gets the human-readable explanation.
    /// </summary>
    public string Reason { get; }
}

/// <summary>
/// Describes how a single row group participates in a query plan.
/// </summary>
public sealed class RowGroupPlan
{
    internal RowGroupPlan(
        string filePath,
        int index,
        long rowCount,
        bool shouldRead,
        bool pageIndexAvailable,
        IReadOnlyList<RowGroupPredicateDecision> decisions,
        int pageCount,
        int selectedPageCount,
        long candidateRowCountUpperBound,
        bool usedFallbackPageIndex,
        string pagePruningSource,
        string pagePruningReason,
        IReadOnlyList<RowInterval> candidateIntervals)
    {
        FilePath = filePath;
        Index = index;
        RowCount = rowCount;
        ShouldRead = shouldRead;
        PageIndexAvailable = pageIndexAvailable;
        Decisions = decisions;
        PageCount = pageCount;
        SelectedPageCount = selectedPageCount;
        CandidateRowCountUpperBound = candidateRowCountUpperBound;
        UsedFallbackPageIndex = usedFallbackPageIndex;
        PagePruningSource = pagePruningSource;
        PagePruningReason = pagePruningReason;
        CandidateIntervals = candidateIntervals;
    }

    /// <summary>
    /// Gets the parquet file path that owns the row group.
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Gets the zero-based row-group index.
    /// </summary>
    public int Index { get; }

    /// <summary>
    /// Gets the total number of rows in the row group.
    /// </summary>
    public long RowCount { get; }

    /// <summary>
    /// Gets a value indicating whether the row group will be read.
    /// </summary>
    public bool ShouldRead { get; }

    /// <summary>
    /// Gets a value indicating whether page indexes are available for the row group.
    /// </summary>
    public bool PageIndexAvailable { get; }

    /// <summary>
    /// Gets the predicate decisions made for the row group.
    /// </summary>
    public IReadOnlyList<RowGroupPredicateDecision> Decisions { get; }

    /// <summary>
    /// Gets the total number of pages in the row group.
    /// </summary>
    public int PageCount { get; }

    /// <summary>
    /// Gets the number of pages that remain after pruning.
    /// </summary>
    public int SelectedPageCount { get; }

    /// <summary>
    /// Gets an upper bound on the number of candidate rows that remain after pruning.
    /// </summary>
    public long CandidateRowCountUpperBound { get; }

    /// <summary>
    /// Gets a value indicating whether fallback page metadata was used.
    /// </summary>
    public bool UsedFallbackPageIndex { get; }

    /// <summary>
    /// Gets the page-pruning component that produced the row-group result.
    /// </summary>
    public string PagePruningSource { get; }

    /// <summary>
    /// Gets the human-readable page-pruning explanation.
    /// </summary>
    public string PagePruningReason { get; }

    internal IReadOnlyList<RowInterval> CandidateIntervals { get; }
}

/// <summary>
/// Describes the result of evaluating a predicate against a row group.
/// </summary>
public sealed class RowGroupPredicateDecision
{
    /// <summary>
    /// Initializes a new row-group predicate decision.
    /// </summary>
    /// <param name="predicate">The predicate description.</param>
    /// <param name="mayMatch">Whether the row group may match the predicate.</param>
    /// <param name="source">The component that made the decision.</param>
    /// <param name="reason">A human-readable explanation.</param>
    public RowGroupPredicateDecision(string predicate, bool mayMatch, string source, string reason)
    {
        Predicate = predicate;
        MayMatch = mayMatch;
        Source = source;
        Reason = reason;
    }

    /// <summary>
    /// Gets the predicate description.
    /// </summary>
    public string Predicate { get; }

    /// <summary>
    /// Gets a value indicating whether the row group may match the predicate.
    /// </summary>
    public bool MayMatch { get; }

    /// <summary>
    /// Gets the component that made the decision.
    /// </summary>
    public string Source { get; }

    /// <summary>
    /// Gets the human-readable explanation.
    /// </summary>
    public string Reason { get; }
}
