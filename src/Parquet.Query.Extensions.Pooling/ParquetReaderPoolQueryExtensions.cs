using Parquet.Query;

namespace Parquet.Query.Extensions.Pooling;

/// <summary>
/// Adds query helpers for the reader pool extension package.
/// </summary>
public static class ParquetReaderPoolQueryExtensions
{
    /// <summary>
    /// Routes query planning and execution through the provided reader pool.
    /// </summary>
    public static ParquetQuery<TSource, TResult> WithReaderPool<TSource, TResult>(
        this ParquetQuery<TSource, TResult> query,
        ParquetReaderPool pool)
        where TSource : class, new()
    {
        Guard.NotNull(query, nameof(query));
        Guard.NotNull(pool, nameof(pool));
        return query.WithReaderFactory(pool);
    }
}
