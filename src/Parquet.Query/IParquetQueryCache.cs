namespace Parquet.Query;

/// <summary>
/// Stores reusable query planning artifacts for repeated parquet queries.
/// </summary>
public interface IParquetQueryCache
{
    /// <summary>
    /// Tries to read a cached value for the supplied key.
    /// </summary>
    ValueTask<object?> GetAsync(
        string key,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Stores a cached value for the supplied key.
    /// </summary>
    ValueTask SetAsync(
        string key,
        object value,
        CancellationToken cancellationToken = default);
}
