namespace Parquet.Query.Extensions.Pooling;

/// <summary>
/// Holds an exclusive block for a pooled parquet file until disposed.
/// </summary>
public sealed class ParquetReaderPoolFileBlockLease : IAsyncDisposable
{
    private readonly ParquetReaderPool _pool;
    private readonly string _filePath;
    private readonly ParquetReaderPool.FilePoolState _state;
    private int _disposed;

    internal ParquetReaderPoolFileBlockLease(
        ParquetReaderPool pool,
        string filePath,
        ParquetReaderPool.FilePoolState state)
    {
        _pool = pool ?? throw new ArgumentNullException(nameof(pool));
        _filePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        _state = state ?? throw new ArgumentNullException(nameof(state));
    }

    /// <summary>
    /// Gets the normalized file path that is currently blocked.
    /// </summary>
    public string FilePath => _filePath;

    /// <summary>
    /// Releases the file block and allows new reader rentals.
    /// </summary>
    public ValueTask DisposeAsync() =>
        Interlocked.Exchange(ref _disposed, 1) == 0
            ? _pool.ReleaseFileBlockAsync(_state)
            : ValueTaskCompatibility.CompletedTask;
}
