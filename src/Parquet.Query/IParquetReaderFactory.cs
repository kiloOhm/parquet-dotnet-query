using Parquet;

namespace Parquet.Query;

/// <summary>
/// Opens parquet readers for query planning and execution.
/// </summary>
public interface IParquetReaderFactory
{
    /// <summary>
    /// Rents a reader lease for the specified file.
    /// </summary>
    ValueTask<IParquetReaderLease> RentAsync(
        string filePath,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default);
}
