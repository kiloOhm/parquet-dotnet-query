namespace Parquet.Query.Internal;

internal sealed class NullParquetQueryCache : IParquetQueryCache
{
    public static NullParquetQueryCache Instance { get; } = new();

    private NullParquetQueryCache()
    {
    }

    public ValueTask<object?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrWhiteSpace(key, nameof(key));
        return ValueTaskCompatibility.FromResult<object?>(null);
    }

    public ValueTask SetAsync(string key, object value, CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrWhiteSpace(key, nameof(key));
        Guard.NotNull(value, nameof(value));
        return ValueTaskCompatibility.CompletedTask;
    }
}
