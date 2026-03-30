using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Indexing;
using Parquet.Query.Internal;

namespace Parquet.Query.Extensions.Indexing;

/// <summary>
/// Builds a hash-bucket index and stores it in parquet footer metadata.
/// </summary>
public sealed class FooterHashIndexingStrategy : IParquetIndexingStrategy
{
    /// <summary>
    /// Initializes a new footer hash indexing strategy.
    /// </summary>
    /// <param name="bucketCount">The number of hash buckets to use.</param>
    public FooterHashIndexingStrategy(int bucketCount = 1024)
    {
        if (bucketCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bucketCount), "Bucket count must be positive.");
        }

        BucketCount = bucketCount;
    }

    /// <inheritdoc />
    public string Name => FooterIndexNames.HashStrategyName;

    /// <summary>
    /// Gets the number of hash buckets used by the index.
    /// </summary>
    public int BucketCount { get; }

    /// <inheritdoc />
    public bool CanHandle(ParquetIndexDescriptor descriptor) =>
        descriptor.Kind == ParquetIndexKind.External &&
        string.Equals(descriptor.StrategyName, FooterIndexNames.HashStrategyName, StringComparison.OrdinalIgnoreCase);

    /// <inheritdoc />
    public async ValueTask ApplyAsync(
        ParquetIndexingContext context,
        ParquetIndexDescriptor descriptor,
        CancellationToken cancellationToken = default)
    {
        if (context is null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (descriptor is null)
        {
            throw new ArgumentNullException(nameof(descriptor));
        }

        var parquetOptions = ParquetOptionsFactory.Clone(context.WritePlan.SerializerOptions.ParquetOptions);
        var index = await BuildIndexAsync(context.FilePath, descriptor.ColumnPath, parquetOptions, BucketCount, cancellationToken).ConfigureAwait(false);
        var metadataKey = FooterIndexStorage.GetHashMetadataKey(descriptor.ColumnPath);
        var metadataValue = FooterIndexStorage.Serialize(index);
        await FooterIndexStorage.WriteToFooterAsync(context.FilePath, metadataKey, metadataValue, parquetOptions, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<FooterHashIndexModel> BuildIndexAsync(
        string filePath,
        string columnPath,
        Parquet.ParquetOptions? parquetOptions,
        int bucketCount,
        CancellationToken cancellationToken)
    {
        using var stream = System.IO.File.OpenRead(filePath);
        using var reader = await Parquet.ParquetReader.CreateAsync(
            stream,
            parquetOptions,
            leaveStreamOpen: false,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        var dataField = reader.Schema.GetDataFields()
            .FirstOrDefault(field => string.Equals(field.Path.ToString(), columnPath, StringComparison.Ordinal));
        if (dataField is null)
        {
            throw new InvalidOperationException($"Column '{columnPath}' was not found in '{filePath}'.");
        }

        var fieldType = Nullable.GetUnderlyingType(dataField.ClrType) ?? dataField.ClrType;
        if (!FooterIndexValueFormatter.IsSupportedType(fieldType))
        {
            FooterIndexDiagnostics.WarnHashUnsupportedType(columnPath, fieldType);
            throw new InvalidOperationException($"Footer hash indexes do not support '{fieldType.Name}' columns.");
        }

        var buckets = new Dictionary<int, HashSet<int>>();
        HashSet<string>? distinctValues = new(StringComparer.Ordinal);
        for (var rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++)
        {
            using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
            var column = await rowGroupReader.ReadColumnAsync(dataField, cancellationToken).ConfigureAwait(false);
            var rowGroupBuckets = new HashSet<int>();

            for (var index = 0; index < column.Data.Length; index++)
            {
                if (!FooterIndexValueFormatter.TryFormat(column.Data.GetValue(index), out var value))
                {
                    continue;
                }

                rowGroupBuckets.Add(FooterIndexValueFormatter.GetBucket(value, bucketCount));
                if (distinctValues is not null)
                {
                    distinctValues.Add(value);
                    if (distinctValues.Count > FooterIndexDiagnostics.RecommendedBitmapDistinctValueThreshold)
                    {
                        distinctValues = null;
                    }
                }
            }

            foreach (var bucket in rowGroupBuckets)
            {
                if (!buckets.TryGetValue(bucket, out var rowGroups))
                {
                    rowGroups = new HashSet<int>();
                    buckets[bucket] = rowGroups;
                }

                rowGroups.Add(rowGroupIndex);
            }
        }

        if (distinctValues is not null)
        {
            FooterIndexDiagnostics.WarnHashLowCardinality(columnPath, fieldType, distinctValues.Count);
        }

        return new FooterHashIndexModel
        {
            ColumnPath = columnPath,
            BucketCount = bucketCount,
            Buckets = buckets
                .OrderBy(entry => entry.Key)
                .Select(entry => new FooterHashBucketModel
                {
                    Bucket = entry.Key,
                    RowGroups = entry.Value.OrderBy(rowGroup => rowGroup).ToArray()
                })
                .ToList()
        };
    }
}
