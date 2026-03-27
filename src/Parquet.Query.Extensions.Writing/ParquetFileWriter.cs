using Parquet.Query.Extensions.Writing.Indexing;
using Parquet.Serialization;

namespace Parquet.Query.Extensions.Writing;

public static class ParquetFileWriter
{
    public static Task<ParquetWritePlan> WriteAsync<T>(
        IEnumerable<T> rows,
        string filePath,
        IEnumerable<IParquetIndexingStrategy>? indexingStrategies = null,
        ParquetSerializerOptions? serializerOptions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

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

    public static async Task<ParquetWritePlan> WriteAsync<T>(
        IEnumerable<T> rows,
        string filePath,
        ParquetWritePlan writePlan,
        IEnumerable<IParquetIndexingStrategy>? indexingStrategies = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);
        ArgumentNullException.ThrowIfNull(writePlan);

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
