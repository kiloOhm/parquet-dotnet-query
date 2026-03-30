namespace Parquet.Query.Extensions.Writing.Indexing;

/// <summary>
/// Provides context to indexing strategies after a parquet file has been written.
/// </summary>
public sealed class ParquetIndexingContext
{
    /// <summary>
    /// Initializes a new indexing context.
    /// </summary>
    /// <param name="filePath">The parquet file path.</param>
    /// <param name="writePlan">The write plan used to create the file.</param>
    public ParquetIndexingContext(string filePath, ParquetWritePlan writePlan)
    {
        FilePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        WritePlan = writePlan ?? throw new ArgumentNullException(nameof(writePlan));
    }

    /// <summary>
    /// Gets the parquet file path.
    /// </summary>
    public string FilePath { get; }

    /// <summary>
    /// Gets the write plan used to create the file.
    /// </summary>
    public ParquetWritePlan WritePlan { get; }
}
