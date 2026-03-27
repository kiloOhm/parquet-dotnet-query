namespace Parquet.Query.Extensions.Writing.Indexing;

public interface IParquetIndexingStrategy
{
    string Name { get; }

    bool CanHandle(ParquetIndexDescriptor descriptor);

    ValueTask ApplyAsync(ParquetIndexingContext context, ParquetIndexDescriptor descriptor, CancellationToken cancellationToken = default);
}
