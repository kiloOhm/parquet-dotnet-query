using Parquet.Query.Extensions.Writing;
using Parquet.Query.Extensions.Writing.Indexing;

namespace Parquet.Query.Extensions.Indexing;

public sealed class FooterHashIndexingStrategy : IParquetIndexingStrategy
{
    public FooterHashIndexingStrategy(int bucketCount = 1024)
    {
        if (bucketCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bucketCount), "Bucket count must be positive.");
        }

        BucketCount = bucketCount;
    }

    public string Name => FooterIndexNames.HashStrategyName;

    public int BucketCount { get; }

    public bool CanHandle(ParquetIndexDescriptor descriptor) =>
        descriptor.Kind == ParquetIndexKind.External &&
        string.Equals(descriptor.StrategyName, FooterIndexNames.HashStrategyName, StringComparison.OrdinalIgnoreCase);

    public async ValueTask ApplyAsync(
        ParquetIndexingContext context,
        ParquetIndexDescriptor descriptor,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(descriptor);

        var index = await BuildIndexAsync(context.FilePath, descriptor.ColumnPath, BucketCount, cancellationToken).ConfigureAwait(false);
        var metadataKey = FooterIndexStorage.GetHashMetadataKey(descriptor.ColumnPath);
        var metadataValue = FooterIndexStorage.Serialize(index);
        await FooterIndexStorage.WriteToFooterAsync(context.FilePath, metadataKey, metadataValue, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<FooterHashIndexModel> BuildIndexAsync(
        string filePath,
        string columnPath,
        int bucketCount,
        CancellationToken cancellationToken)
    {
        await using var stream = System.IO.File.OpenRead(filePath);
        using var reader = await Parquet.ParquetReader.CreateAsync(stream, leaveStreamOpen: false, cancellationToken: cancellationToken).ConfigureAwait(false);

        var dataField = reader.Schema.GetDataFields()
            .FirstOrDefault(field => string.Equals(field.Path.ToString(), columnPath, StringComparison.Ordinal));
        if (dataField is null)
        {
            throw new InvalidOperationException($"Column '{columnPath}' was not found in '{filePath}'.");
        }

        var fieldType = Nullable.GetUnderlyingType(dataField.ClrType) ?? dataField.ClrType;
        if (fieldType != typeof(string))
        {
            throw new InvalidOperationException($"Footer hash indexes currently support string columns only. Column '{columnPath}' is '{fieldType.Name}'.");
        }

        var buckets = new Dictionary<int, HashSet<int>>();
        for (var rowGroupIndex = 0; rowGroupIndex < reader.RowGroupCount; rowGroupIndex++)
        {
            using var rowGroupReader = reader.OpenRowGroupReader(rowGroupIndex);
            var column = await rowGroupReader.ReadColumnAsync(dataField, cancellationToken).ConfigureAwait(false);
            var rowGroupBuckets = new HashSet<int>();

            for (var index = 0; index < column.Data.Length; index++)
            {
                if (column.Data.GetValue(index) is not string value)
                {
                    continue;
                }

                rowGroupBuckets.Add(FooterIndexValueFormatter.GetBucket(value, bucketCount));
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
