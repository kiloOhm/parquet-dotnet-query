using Parquet.Query;

namespace Parquet.Query.Extensions.Indexing;

public static class FooterIndexQueryExtensions
{
    public static ParquetQuery<TSource, TResult> WithFooterIndexes<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query)
        where TSource : class, new()
    {
        ArgumentNullException.ThrowIfNull(query);
        return query.WithPredicatePlanner(FooterIndexPredicatePlanner<TSource>.Instance);
    }
}
