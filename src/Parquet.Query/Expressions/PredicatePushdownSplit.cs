using System.Linq.Expressions;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Expressions;

public sealed class PredicatePushdownSplit<T>
{
    public PredicatePushdownSplit(
        PushdownFilter<T> pushdownFilter,
        Expression<Func<T, bool>>? residualPredicate,
        IReadOnlyList<string> unsupportedExpressions)
    {
        PushdownFilter = pushdownFilter;
        ResidualPredicate = residualPredicate;
        UnsupportedExpressions = unsupportedExpressions;
    }

    public PushdownFilter<T> PushdownFilter { get; }

    public Expression<Func<T, bool>>? ResidualPredicate { get; }

    public IReadOnlyList<string> UnsupportedExpressions { get; }
}
