namespace Parquet.Query.Planning;

public sealed class ParquetQueryPlan
{
    public ParquetQueryPlan(
        IReadOnlyList<QueryFilePlan> files,
        IReadOnlyList<string> pushdownPredicates,
        IReadOnlyList<string> residualPredicates,
        IReadOnlyList<string> readColumns,
        IReadOnlyList<string> filterColumns,
        IReadOnlyList<string> deferredColumns,
        bool requiresFullMaterialization)
    {
        Files = files;
        PushdownPredicates = pushdownPredicates;
        ResidualPredicates = residualPredicates;
        ReadColumns = readColumns;
        FilterColumns = filterColumns;
        DeferredColumns = deferredColumns;
        RequiresFullMaterialization = requiresFullMaterialization;
    }

    public string FilePath => Files.Count == 1 ? Files[0].FilePath : string.Empty;

    public IReadOnlyList<QueryFilePlan> Files { get; }

    public IReadOnlyList<string> PushdownPredicates { get; }

    public IReadOnlyList<string> ResidualPredicates { get; }

    public IReadOnlyList<string> ReadColumns { get; }

    public IReadOnlyList<string> FilterColumns { get; }

    public IReadOnlyList<string> DeferredColumns { get; }

    public bool RequiresFullMaterialization { get; }

    public bool UsesLateMaterialization => !RequiresFullMaterialization && DeferredColumns.Count > 0;

    public bool PageIndexAvailable => Files.Any(file => file.PageIndexAvailable);

    public IReadOnlyList<RowGroupPlan> RowGroups => Files.SelectMany(file => file.RowGroups).ToArray();

    public int SelectedFileCount => Files.Count(file => file.ShouldRead);

    public int SelectedRowGroupCount => RowGroups.Count(rowGroup => rowGroup.ShouldRead);

    public long SelectedRowCountUpperBound => RowGroups.Where(rowGroup => rowGroup.ShouldRead).Sum(rowGroup => rowGroup.CandidateRowCountUpperBound);
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
    public RowGroupPlan(
        string filePath,
        int index,
        long rowCount,
        bool shouldRead,
        bool pageIndexAvailable,
        IReadOnlyList<RowGroupPredicateDecision> decisions,
        int pageCount,
        int selectedPageCount,
        long candidateRowCountUpperBound,
        bool usedFallbackPageIndex)
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
