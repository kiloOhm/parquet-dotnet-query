namespace Parquet.Query.Planning;

public sealed class ParquetQueryPlan
{
    private readonly IReadOnlyList<RowGroupPlan> _rowGroups;

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

    public string FilePath => Files.Count == 1 ? Files[0].FilePath : string.Empty;

    public IReadOnlyList<QueryFilePlan> Files { get; }

    public IReadOnlyList<string> PushdownPredicates { get; }

    public IReadOnlyList<string> ResidualPredicates { get; }

    public IReadOnlyList<string> ResidualPredicateDiagnostics { get; }

    public IReadOnlyList<string> ReadColumns { get; }

    public IReadOnlyList<string> FilterColumns { get; }

    public IReadOnlyList<string> DeferredColumns { get; }

    public bool RequiresFullMaterialization { get; }

    public bool UsesLateMaterialization => !RequiresFullMaterialization && DeferredColumns.Count > 0;

    public bool PageIndexAvailable => Files.Any(file => file.PageIndexAvailable);

    public bool UsedFallbackPageIndex => RowGroups.Any(rowGroup => rowGroup.UsedFallbackPageIndex);

    public IReadOnlyList<RowGroupPlan> RowGroups => _rowGroups;

    public int SelectedFileCount => Files.Count(file => file.ShouldRead);

    public int SelectedRowGroupCount => RowGroups.Count(rowGroup => rowGroup.ShouldRead);

    public long SelectedRowCountUpperBound => RowGroups.Where(rowGroup => rowGroup.ShouldRead).Sum(rowGroup => rowGroup.CandidateRowCountUpperBound);

    public int SelectedPageCount => RowGroups.Where(rowGroup => rowGroup.ShouldRead).Sum(rowGroup => rowGroup.SelectedPageCount);

    public int TotalPageCount => RowGroups.Sum(rowGroup => rowGroup.PageCount);
}

public sealed class QueryFilePlan
{
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

    public string FilePath { get; }

    public bool ShouldRead { get; }

    public string Reason { get; }

    public IReadOnlyList<FilePredicateDecision> Decisions { get; }

    public IReadOnlyList<RowGroupPlan> RowGroups { get; }

    public bool PageIndexAvailable { get; }

    public int SelectedRowGroupCount => RowGroups.Count(rowGroup => rowGroup.ShouldRead);
}

public sealed class FilePredicateDecision
{
    public FilePredicateDecision(string predicate, bool mayMatch, string source, string reason)
    {
        Predicate = predicate;
        MayMatch = mayMatch;
        Source = source;
        Reason = reason;
    }

    public string Predicate { get; }

    public bool MayMatch { get; }

    public string Source { get; }

    public string Reason { get; }
}

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

    public string FilePath { get; }

    public int Index { get; }

    public long RowCount { get; }

    public bool ShouldRead { get; }

    public bool PageIndexAvailable { get; }

    public IReadOnlyList<RowGroupPredicateDecision> Decisions { get; }

    public int PageCount { get; }

    public int SelectedPageCount { get; }

    public long CandidateRowCountUpperBound { get; }

    public bool UsedFallbackPageIndex { get; }

    public string PagePruningSource { get; }

    public string PagePruningReason { get; }

    internal IReadOnlyList<RowInterval> CandidateIntervals { get; }
}

public sealed class RowGroupPredicateDecision
{
    public RowGroupPredicateDecision(string predicate, bool mayMatch, string source, string reason)
    {
        Predicate = predicate;
        MayMatch = mayMatch;
        Source = source;
        Reason = reason;
    }

    public string Predicate { get; }

    public bool MayMatch { get; }

    public string Source { get; }

    public string Reason { get; }
}
