using System.Linq.Expressions;

namespace Parquet.Query.Pushdown;

/// <summary>
/// Builds a conjunction of pushdown predicates for a parquet query.
/// </summary>
/// <typeparam name="T">The source row type the predicates target.</typeparam>
public sealed class PushdownFilterBuilder<T>
{
    private readonly List<PushdownPredicate<T>> _predicates = new();

    /// <summary>
    /// Adds a pre-built predicate to the filter.
    /// </summary>
    /// <param name="predicate">The predicate to add.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> Add(PushdownPredicate<T> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);
        _predicates.Add(predicate);
        return this;
    }

    /// <summary>
    /// Adds an equality predicate.
    /// </summary>
    /// <typeparam name="TValue">The value type being compared.</typeparam>
    /// <param name="selector">The source member to compare.</param>
    /// <param name="value">The comparison value.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> Eq<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.Equal, value));
        return this;
    }

    /// <summary>
    /// Adds an inequality predicate.
    /// </summary>
    /// <typeparam name="TValue">The value type being compared.</typeparam>
    /// <param name="selector">The source member to compare.</param>
    /// <param name="value">The comparison value.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> NotEq<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.NotEqual, value));
        return this;
    }

    /// <summary>
    /// Adds a less-than predicate.
    /// </summary>
    /// <typeparam name="TValue">The value type being compared.</typeparam>
    /// <param name="selector">The source member to compare.</param>
    /// <param name="value">The comparison value.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> Lt<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.LessThan, value));
        return this;
    }

    /// <summary>
    /// Adds a less-than-or-equal predicate.
    /// </summary>
    /// <typeparam name="TValue">The value type being compared.</typeparam>
    /// <param name="selector">The source member to compare.</param>
    /// <param name="value">The comparison value.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> Le<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.LessThanOrEqual, value));
        return this;
    }

    /// <summary>
    /// Adds a greater-than predicate.
    /// </summary>
    /// <typeparam name="TValue">The value type being compared.</typeparam>
    /// <param name="selector">The source member to compare.</param>
    /// <param name="value">The comparison value.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> Gt<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.GreaterThan, value));
        return this;
    }

    /// <summary>
    /// Adds a greater-than-or-equal predicate.
    /// </summary>
    /// <typeparam name="TValue">The value type being compared.</typeparam>
    /// <param name="selector">The source member to compare.</param>
    /// <param name="value">The comparison value.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> Ge<TValue>(Expression<Func<T, TValue>> selector, TValue value)
    {
        _predicates.Add(PushdownPredicateFactory.CreateComparison<T>(selector, ComparisonOperator.GreaterThanOrEqual, value));
        return this;
    }

    /// <summary>
    /// Adds inclusive lower and upper bound predicates for the same source member.
    /// </summary>
    /// <typeparam name="TValue">The value type being compared.</typeparam>
    /// <param name="selector">The source member to compare.</param>
    /// <param name="inclusiveLowerBound">The inclusive lower bound.</param>
    /// <param name="inclusiveUpperBound">The inclusive upper bound.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> Between<TValue>(
        Expression<Func<T, TValue>> selector,
        TValue inclusiveLowerBound,
        TValue inclusiveUpperBound) =>
        Ge(selector, inclusiveLowerBound).Le(selector, inclusiveUpperBound);

    /// <summary>
    /// Adds a string prefix predicate.
    /// </summary>
    /// <param name="selector">The string member to compare.</param>
    /// <param name="prefix">The required prefix.</param>
    /// <returns>The current builder.</returns>
    public PushdownFilterBuilder<T> StartsWith(Expression<Func<T, string?>> selector, string prefix)
    {
        _predicates.Add(PushdownPredicateFactory.CreateStartsWith(selector, prefix));
        return this;
    }

    /// <summary>
    /// Creates an immutable pushdown filter from the accumulated predicates.
    /// </summary>
    /// <returns>The built pushdown filter.</returns>
    public PushdownFilter<T> Build() => new(_predicates);
}
