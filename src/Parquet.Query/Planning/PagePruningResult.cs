namespace Parquet.Query.Planning;

/// <summary>
/// Represents a half-open interval of candidate row indexes.
/// </summary>
/// <param name="Start">The inclusive start row index.</param>
/// <param name="End">The exclusive end row index.</param>
public sealed record RowInterval(long Start, long End);

/// <summary>
/// Describes the rows and pages that remain after page pruning.
/// </summary>
public sealed class PagePruningResult
{
    /// <summary>
    /// Creates a result that covers the full row group because no page pruning was applied.
    /// </summary>
    /// <param name="rowCount">The total number of rows in the row group.</param>
    /// <returns>A result that retains the full row group.</returns>
    public static PagePruningResult Full(long rowCount) => new(
        new[] { new RowInterval(0, rowCount) },
        pageCount: 0,
        selectedPageCount: 0,
        pageIndexAvailable: false,
        usedFallbackIndex: false,
        source: "unavailable",
        reason: "Page pruning was not applied.");

    /// <summary>
    /// Initializes a new page pruning result.
    /// </summary>
    /// <param name="intervals">The retained row intervals.</param>
    /// <param name="pageCount">The total number of pages considered.</param>
    /// <param name="selectedPageCount">The number of pages still selected.</param>
    /// <param name="pageIndexAvailable">Whether persisted page indexes were available.</param>
    /// <param name="usedFallbackIndex">Whether fallback page metadata was used.</param>
    /// <param name="source">The pruning strategy that produced the result.</param>
    /// <param name="reason">A human-readable explanation of the result.</param>
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

    /// <summary>
    /// Gets the retained row intervals.
    /// </summary>
    public IReadOnlyList<RowInterval> Intervals { get; }

    /// <summary>
    /// Gets the total number of pages considered.
    /// </summary>
    public int PageCount { get; }

    /// <summary>
    /// Gets the number of pages that remain selected.
    /// </summary>
    public int SelectedPageCount { get; }

    /// <summary>
    /// Gets a value indicating whether persisted page indexes were available.
    /// </summary>
    public bool PageIndexAvailable { get; }

    /// <summary>
    /// Gets a value indicating whether fallback page metadata was used.
    /// </summary>
    public bool UsedFallbackIndex { get; }

    /// <summary>
    /// Gets the pruning strategy that produced the result.
    /// </summary>
    public string Source { get; }

    /// <summary>
    /// Gets a human-readable explanation of the pruning result.
    /// </summary>
    public string Reason { get; }

    /// <summary>
    /// Gets an upper bound on the number of candidate rows that remain after pruning.
    /// </summary>
    public long CandidateRowCountUpperBound => Intervals.Sum(interval => interval.End - interval.Start);
}
