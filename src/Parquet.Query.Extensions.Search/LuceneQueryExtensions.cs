using System.Linq.Expressions;
using Parquet.Query;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Extensions.Search;

/// <summary>
/// Adds Lucene-style term search helpers to pushdown filters and parquet queries.
/// </summary>
public static class LuceneQueryExtensions
{
    /// <summary>
    /// Adds an exact Lucene term match predicate to a pushdown filter builder.
    /// </summary>
    /// <typeparam name="TSource">The source row type.</typeparam>
    /// <param name="builder">The builder to configure.</param>
    /// <param name="selector">The string member to search.</param>
    /// <param name="term">The term to match after analysis.</param>
    /// <returns>The current builder.</returns>
    public static PushdownFilterBuilder<TSource> LuceneMatch<TSource>(
        this PushdownFilterBuilder<TSource> builder,
        Expression<Func<TSource, string?>> selector,
        string term)
        where TSource : class, new()
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }
        return builder.Add(new LuceneTermPredicate<TSource>(selector, term));
    }

    /// <summary>
    /// Adds a fuzzy Lucene term predicate to a pushdown filter builder.
    /// </summary>
    /// <typeparam name="TSource">The source row type.</typeparam>
    /// <param name="builder">The builder to configure.</param>
    /// <param name="selector">The string member to search.</param>
    /// <param name="term">The term to match after analysis.</param>
    /// <param name="maxEdits">The maximum edit distance allowed.</param>
    /// <param name="prefixLength">The number of leading characters that must match exactly.</param>
    /// <param name="transpositions">Whether transposed characters count as a single edit.</param>
    /// <returns>The current builder.</returns>
    public static PushdownFilterBuilder<TSource> LuceneFuzzy<TSource>(
        this PushdownFilterBuilder<TSource> builder,
        Expression<Func<TSource, string?>> selector,
        string term,
        int maxEdits = 1,
        int prefixLength = 0,
        bool transpositions = true)
        where TSource : class, new()
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }
        return builder.Add(new LuceneTermPredicate<TSource>(selector, term, maxEdits, prefixLength, transpositions));
    }

    /// <summary>
    /// Enables Lucene footer indexes for query planning.
    /// </summary>
    /// <typeparam name="TSource">The source row type.</typeparam>
    /// <typeparam name="TResult">The query result type.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <returns>A new query that uses Lucene footer index pruning.</returns>
    public static ParquetQuery<TSource, TResult> WithLuceneSearch<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query)
        where TSource : class, new()
    {
        if (query is null)
        {
            throw new ArgumentNullException(nameof(query));
        }
        return query.WithPredicatePlanner(LucenePredicatePlanner<TSource>.Instance);
    }

    /// <summary>
    /// Adds an exact Lucene term predicate to a query and enables Lucene planning.
    /// </summary>
    /// <typeparam name="TSource">The source row type.</typeparam>
    /// <typeparam name="TResult">The query result type.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <param name="selector">The string member to search.</param>
    /// <param name="term">The term to match after analysis.</param>
    /// <returns>A new query with Lucene term matching enabled.</returns>
    public static ParquetQuery<TSource, TResult> LuceneMatch<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query,
        Expression<Func<TSource, string?>> selector,
        string term)
        where TSource : class, new()
    {
        if (query is null)
        {
            throw new ArgumentNullException(nameof(query));
        }
        return query
            .WithLuceneSearch()
            .Pushdown(filter => filter.LuceneMatch(selector, term));
    }

    /// <summary>
    /// Adds a fuzzy Lucene term predicate to a query and enables Lucene planning.
    /// </summary>
    /// <typeparam name="TSource">The source row type.</typeparam>
    /// <typeparam name="TResult">The query result type.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <param name="selector">The string member to search.</param>
    /// <param name="term">The term to match after analysis.</param>
    /// <param name="maxEdits">The maximum edit distance allowed.</param>
    /// <param name="prefixLength">The number of leading characters that must match exactly.</param>
    /// <param name="transpositions">Whether transposed characters count as a single edit.</param>
    /// <returns>A new query with Lucene fuzzy matching enabled.</returns>
    public static ParquetQuery<TSource, TResult> LuceneFuzzy<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query,
        Expression<Func<TSource, string?>> selector,
        string term,
        int maxEdits = 1,
        int prefixLength = 0,
        bool transpositions = true)
        where TSource : class, new()
    {
        if (query is null)
        {
            throw new ArgumentNullException(nameof(query));
        }
        return query
            .WithLuceneSearch()
            .Pushdown(filter => filter.LuceneFuzzy(selector, term, maxEdits, prefixLength, transpositions));
    }
}
