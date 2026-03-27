namespace Parquet.Query.Planning;

public sealed record RowInterval(long Start, long End);

public sealed class PagePruningResult
{
    public static PagePruningResult Full(long rowCount) => new(
        new[] { new RowInterval(0, rowCount) },
        pageCount: 0,
        selectedPageCount: 0,
        pageIndexAvailable: false,
        usedFallbackIndex: false,
        source: "unavailable",
        reason: "Page pruning was not applied.");

    public PagePruningResult(
        IReadOnlyList<RowInterval> intervals,
        int pageCount,
        int selectedPageCount,
        bool pageIndexAvailable,
        bool usedFallbackIndex,
        string source,
        string reason)
    {
        Intervals = intervals ?? throw new ArgumentNullException(nameof(intervals));
        PageCount = pageCount;
        SelectedPageCount = selectedPageCount;
        PageIndexAvailable = pageIndexAvailable;
        UsedFallbackIndex = usedFallbackIndex;
        Source = source ?? throw new ArgumentNullException(nameof(source));
        Reason = reason ?? throw new ArgumentNullException(nameof(reason));
    }

    public IReadOnlyList<RowInterval> Intervals { get; }

    public int PageCount { get; }

    public int SelectedPageCount { get; }

    public bool PageIndexAvailable { get; }

    public bool UsedFallbackIndex { get; }

    public string Source { get; }

    public string Reason { get; }

    public long CandidateRowCountUpperBound => Intervals.Sum(interval => interval.End - interval.Start);
}
