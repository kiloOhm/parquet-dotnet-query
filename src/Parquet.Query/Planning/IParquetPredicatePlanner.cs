using Parquet.Query.Pushdown;

namespace Parquet.Query.Planning;

public interface IParquetPredicatePlanner<TSource>
    where TSource : class, new()
{
    bool CanPlan(PushdownPredicate<TSource> predicate);

    RowGroupPredicateDecision? TryEvaluateRowGroup(
        ParquetRowGroupPlannerContext context,
        PushdownPredicate<TSource> predicate);

    ValueTask<PagePruningResult?> TryPrunePagesAsync(
        ParquetPagePruningContext context,
        PushdownPredicate<TSource> predicate,
        CancellationToken cancellationToken = default);
}
