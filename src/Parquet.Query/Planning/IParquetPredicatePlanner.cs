using Parquet.Query.Pushdown;

namespace Parquet.Query.Planning;

/// <summary>
/// Provides custom row-group and page pruning logic for pushdown predicates.
/// </summary>
/// <typeparam name="TSource">The source row type the planner targets.</typeparam>
public interface IParquetPredicatePlanner<TSource>
    where TSource : class, new()
{
    /// <summary>
    /// Determines whether the planner can evaluate the supplied predicate.
    /// </summary>
    /// <param name="predicate">The predicate to inspect.</param>
    /// <returns><see langword="true"/> when the planner can evaluate the predicate; otherwise <see langword="false"/>.</returns>
    bool CanPlan(PushdownPredicate<TSource> predicate);

    /// <summary>
    /// Attempts to evaluate whether a row group may satisfy the predicate.
    /// </summary>
    /// <param name="context">The row-group planning context.</param>
    /// <param name="predicate">The predicate to evaluate.</param>
    /// <returns>A decision when the planner can evaluate the predicate, or <see langword="null"/> when it cannot contribute.</returns>
    RowGroupPredicateDecision? TryEvaluateRowGroup(
        ParquetRowGroupPlannerContext context,
        PushdownPredicate<TSource> predicate);

    /// <summary>
    /// Attempts to narrow the set of candidate rows within a row group.
    /// </summary>
    /// <param name="context">The page-pruning context.</param>
    /// <param name="predicate">The predicate to evaluate.</param>
    /// <param name="cancellationToken">A token used to cancel pruning work.</param>
    /// <returns>A page pruning result when the planner can contribute, or <see langword="null"/> when it cannot.</returns>
    ValueTask<PagePruningResult?> TryPrunePagesAsync(
        ParquetPagePruningContext context,
        PushdownPredicate<TSource> predicate,
        CancellationToken cancellationToken = default);
}
