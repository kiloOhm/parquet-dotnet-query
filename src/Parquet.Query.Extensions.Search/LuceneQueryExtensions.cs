using System.Linq.Expressions;
using Parquet.Query;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Extensions.Search;

public static class LuceneQueryExtensions
{
    public static PushdownFilterBuilder<TSource> LuceneMatch<TSource>(
        this PushdownFilterBuilder<TSource> builder,
        Expression<Func<TSource, string?>> selector,
        string term)
        where TSource : class, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Add(new LuceneTermPredicate<TSource>(selector, term));
    }

    public static PushdownFilterBuilder<TSource> LuceneFuzzy<TSource>(
        this PushdownFilterBuilder<TSource> builder,
        Expression<Func<TSource, string?>> selector,
        string term,
        int maxEdits = 1,
        int prefixLength = 0,
        bool transpositions = true)
        where TSource : class, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Add(new LuceneTermPredicate<TSource>(selector, term, maxEdits, prefixLength, transpositions));
    }

    public static ParquetQuery<TSource, TResult> WithLuceneSearch<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query)
        where TSource : class, new()
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.WithPredicatePlanner(LucenePredicatePlanner<TSource>.Instance);
    }

    public static ParquetQuery<TSource, TResult> LuceneMatch<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query,
        Expression<Func<TSource, string?>> selector,
        string term)
        where TSource : class, new()
    {
        ArgumentNullException.ThrowIfNull(query);
        return query
            .WithLuceneSearch()
            .Pushdown(filter => filter.LuceneMatch(selector, term));
    }

    public static ParquetQuery<TSource, TResult> LuceneFuzzy<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query,
        Expression<Func<TSource, string?>> selector,
        string term,
        int maxEdits = 1,
        int prefixLength = 0,
        bool transpositions = true)
        where TSource : class, new()
    {
        ArgumentNullException.ThrowIfNull(query);
        return query
            .WithLuceneSearch()
            .Pushdown(filter => filter.LuceneFuzzy(selector, term, maxEdits, prefixLength, transpositions));
    }
}
