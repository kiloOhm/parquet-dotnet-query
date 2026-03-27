namespace Parquet.Query.Extensions.Writing.Indexing;

public sealed class ParquetIndexingContext
{
    public ParquetIndexingContext(string filePath, ParquetWritePlan writePlan)
    {
        FilePath = filePath ?? throw new ArgumentNullException(nameof(filePath));
        WritePlan = writePlan ?? throw new ArgumentNullException(nameof(writePlan));
    }

    public string FilePath { get; }

    public ParquetWritePlan WritePlan { get; }
}
