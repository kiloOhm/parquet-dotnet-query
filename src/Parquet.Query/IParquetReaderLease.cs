using Parquet;

namespace Parquet.Query;

/// <summary>
/// Represents an owned parquet reader that is returned to its source when disposed.
/// </summary>
public interface IParquetReaderLease : IAsyncDisposable
{
    /// <summary>
    /// Gets the leased reader instance.
    /// </summary>
    ParquetReader Reader { get; }
}
