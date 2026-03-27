namespace Parquet.Query.Planning;

public sealed class ParquetQueryPlan
{
    public ParquetQueryPlan(
        string filePath,
        IReadOnlyList<string> pushdownPredicates,
        IReadOnlyList<string> residualPredicates,
        IReadOnlyList<string> readColumns,
        bool requiresFullMaterialization,
        IReadOnlyList<RowGroupPlan> rowGroups)
    {
        FilePath = filePath;
        PushdownPredicates = pushdownPredicates;
        ResidualPredicates = residualPredicates;
        ReadColumns = readColumns;
        RequiresFullMaterialization = requiresFullMaterialization;
        RowGroups = rowGroups;
    }

    public string FilePath { get; }

    public IReadOnlyList<string> PushdownPredicates { get; }

    public IReadOnlyList<string> ResidualPredicates { get; }

    public IReadOnlyList<string> ReadColumns { get; }

    public bool RequiresFullMaterialization { get; }

    public IReadOnlyList<RowGroupPlan> RowGroups { get; }

    public int SelectedRowGroupCount => RowGroups.Count(rowGroup => rowGroup.ShouldRead);

    public long SelectedRowCountUpperBound => RowGroups.Where(rowGroup => rowGroup.ShouldRead).Sum(rowGroup => rowGroup.RowCount);
}

public sealed class RowGroupPlan
{
    public RowGroupPlan(int index, long rowCount, bool shouldRead, IReadOnlyList<RowGroupPredicateDecision> decisions)
    {
        Index = index;
        RowCount = rowCount;
        ShouldRead = shouldRead;
        Decisions = decisions;
    }

    public int Index { get; }

    public long RowCount { get; }

    public bool ShouldRead { get; }

    public IReadOnlyList<RowGroupPredicateDecision> Decisions { get; }
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
