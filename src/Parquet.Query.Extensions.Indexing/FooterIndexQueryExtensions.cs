using Parquet.Query;

namespace Parquet.Query.Extensions.Indexing;

/// <summary>
/// Adds footer-index-based predicate planners to parquet queries.
/// </summary>
public static class FooterIndexQueryExtensions
{
    /// <summary>
    /// Enables footer bitmap indexes for query planning.
    /// </summary>
    /// <typeparam name="TSource">The source row type.</typeparam>
    /// <typeparam name="TResult">The query result type.</typeparam>
    /// <param name="query">The query to configure.</param>
    /// <returns>A new query that uses footer-index-based pruning.</returns>
    public static ParquetQuery<TSource, TResult> WithFooterIndexes<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query)
        where TSource : class, new()
    {
        if (query is null)
        {
            throw new ArgumentNullException(nameof(query));
        }
        return query.WithPredicatePlanner(FooterIndexPredicatePlanner<TSource>.Instance);
    }
}
