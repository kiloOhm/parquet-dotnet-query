namespace Parquet.Query.Extensions.Pooling;

/// <summary>
/// Configures <see cref="ParquetReaderPool"/>.
/// </summary>
public sealed class ParquetReaderPoolOptions
{
    /// <summary>
    /// Gets or sets the maximum number of readers kept alive per file.
    /// </summary>
    public int MaxReadersPerFile { get; set; } = 4;
}
