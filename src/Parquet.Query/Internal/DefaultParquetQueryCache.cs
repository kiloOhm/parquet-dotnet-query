namespace Parquet.Query.Internal;

internal static class DefaultParquetQueryCache
{
    public static IParquetQueryCache Instance { get; } = new LruParquetQueryCache(capacity: 256);
}
