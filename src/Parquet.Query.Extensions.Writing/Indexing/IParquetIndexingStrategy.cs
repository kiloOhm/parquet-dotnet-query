namespace Parquet.Query.Extensions.Writing.Indexing;

/// <summary>
/// Creates additional indexes for columns after a parquet file has been written.
/// </summary>
public interface IParquetIndexingStrategy
{
    /// <summary>
    /// Gets the unique name of the indexing strategy.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// Determines whether the strategy can handle the supplied index descriptor.
    /// </summary>
    /// <param name="descriptor">The descriptor to inspect.</param>
    /// <returns><see langword="true"/> when the strategy can build the index; otherwise <see langword="false"/>.</returns>
    bool CanHandle(ParquetIndexDescriptor descriptor);

    /// <summary>
    /// Applies the indexing strategy to the written parquet file.
    /// </summary>
    /// <param name="context">The file and write-plan context.</param>
    /// <param name="descriptor">The index descriptor to apply.</param>
    /// <param name="cancellationToken">A token used to cancel indexing.</param>
    ValueTask ApplyAsync(ParquetIndexingContext context, ParquetIndexDescriptor descriptor, CancellationToken cancellationToken = default);
}
