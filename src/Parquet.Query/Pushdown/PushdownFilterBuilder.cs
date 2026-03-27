using System.Linq.Expressions;

namespace Parquet.Query.Pushdown;

public sealed class PushdownFilterBuilder<T>
{
    private readonly List<PushdownPredicate<T>> _predicates = new();

    public PushdownFilterBuilder<T> Add(PushdownPredicate<T> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _predicates.Add(predicate);
        return this;
    }

    public PushdownFilterBuilder<T> Eq<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.Equal, value));
        return this;
    }

    public PushdownFilterBuilder<T> NotEq<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.NotEqual, value));
        return this;
    }

    public PushdownFilterBuilder<T> Lt<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.LessThan, value));
        return this;
    }

    public PushdownFilterBuilder<T> Le<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.LessThanOrEqual, value));
        return this;
    }

    public PushdownFilterBuilder<T> Gt<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.GreaterThan, value));
        return this;
    }

    public PushdownFilterBuilder<T> Ge<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.GreaterThanOrEqual, value));
        return this;
    }

    public PushdownFilterBuilder<T> Between<TValue>(
        Expression<Func<T, TValue>> selector,
        TValue inclusiveLowerBound,
        TValue inclusiveUpperBound) =>
        Ge(selector, inclusiveLowerBound).Le(selector, inclusiveUpperBound);

    public PushdownFilterBuilder<T> StartsWith(Expression<Func<T, string?>> selector, string prefix)
    {
        _predicates.Add(PushdownPredicateFactory.CreateStartsWith(selector, prefix));
        return this;
    }

    public PushdownFilter<T> Build() => new(_predicates);
}
