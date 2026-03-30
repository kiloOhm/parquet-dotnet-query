using Parquet.Query.Extensions.Writing.Indexing;
using Parquet.Serialization;

namespace Parquet.Query.Extensions.Writing;

/// <summary>
/// Writes rows to parquet files and applies any configured indexing strategies.
/// </summary>
public static class ParquetFileWriter
{
    /// <summary>
    /// Serializes rows to a parquet file and builds a write plan from the row type.
    /// </summary>
    /// <typeparam name="T">The row type to serialize.</typeparam>
    /// <param name="rows">The rows to write.</param>
    /// <param name="filePath">The destination parquet file path.</param>
    /// <param name="indexingStrategies">Optional indexing strategies to run after serialization.</param>
    /// <param name="serializerOptions">Optional serializer overrides for the generated plan.</param>
    /// <param name="cancellationToken">A token used to cancel the write.</param>
    /// <returns>The write plan used for serialization.</returns>
    public static Task<ParquetWritePlan> WriteAsync<T>(
        IEnumerable<T> rows,
        string filePath,
        IEnumerable<IParquetIndexingStrategy>? indexingStrategies = null,
        ParquetSerializerOptions? serializerOptions = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(rows, nameof(rows));
        Guard.NotNullOrWhiteSpace(filePath, nameof(filePath));

        return WriteCoreAsync(rows, filePath, indexingStrategies, serializerOptions, cancellationToken);
    }

    private static async Task<ParquetWritePlan> WriteCoreAsync<T>(
        IEnumerable<T> rows,
        string filePath,
        IEnumerable<IParquetIndexingStrategy>? indexingStrategies,
        ParquetSerializerOptions? serializerOptions,
        CancellationToken cancellationToken)
    {
        var writePlan = ParquetWritePlanBuilder.Build<T>(serializerOptions);
        return await WriteAsync(rows, filePath, writePlan, indexingStrategies, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Serializes rows to a parquet file by using a precomputed write plan.
    /// </summary>
    /// <typeparam name="T">The row type to serialize.</typeparam>
    /// <param name="rows">The rows to write.</param>
    /// <param name="filePath">The destination parquet file path.</param>
    /// <param name="writePlan">The plan that defines schema, options, and indexes.</param>
    /// <param name="indexingStrategies">Optional indexing strategies to run after serialization.</param>
    /// <param name="cancellationToken">A token used to cancel the write.</param>
    /// <returns>The write plan used for serialization.</returns>
    public static async Task<ParquetWritePlan> WriteAsync<T>(
        IEnumerable<T> rows,
        string filePath,
        ParquetWritePlan writePlan,
        IEnumerable<IParquetIndexingStrategy>? indexingStrategies = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNull(rows, nameof(rows));
        Guard.NotNullOrWhiteSpace(filePath, nameof(filePath));
        Guard.NotNull(writePlan, nameof(writePlan));

        await ParquetSerializer.SerializeAsync(rows, filePath, writePlan.CreateSerializerOptions(), cancellationToken).ConfigureAwait(false);

        if (indexingStrategies is not null)
        {
            var context = new ParquetIndexingContext(filePath, writePlan);
            foreach (var descriptor in writePlan.IndexDescriptors)
            {
                foreach (var strategy in indexingStrategies)
                {
                    if (!strategy.CanHandle(descriptor))
                    {
                        continue;
                    }

                    await strategy.ApplyAsync(context, descriptor, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        return writePlan;
    }
}
