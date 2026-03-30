namespace Parquet.Query.Internal;

internal sealed class NullParquetQueryCache : IParquetQueryCache
{
    public static NullParquetQueryCache Instance { get; } = new();

    private NullParquetQueryCache()
    {
    }

    public ValueTask<object?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        return ValueTask.FromResult<object?>(null);
    }

    public ValueTask SetAsync(string key, object value, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentNullException.ThrowIfNull(value);
        return ValueTask.CompletedTask;
    }
}
