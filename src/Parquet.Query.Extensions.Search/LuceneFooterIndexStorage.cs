using Parquet.Query.Internal;
using Parquet;

namespace Parquet.Query.Extensions.Search;

internal static class LuceneFooterIndexStorage
{
    public static string GetMetadataKey(string columnPath) =>
        LuceneIndexNames.MetadataPrefix + Uri.EscapeDataString(columnPath);

    public static string Serialize(LuceneFooterIndexModel index)
        => ParquetFooterMetadata.Serialize(index);

    public static LuceneFooterIndexModel? TryDeserialize(string? payload)
        => ParquetFooterMetadata.TryDeserialize<LuceneFooterIndexModel>(payload);

    public static async Task WriteToFooterAsync(
        string filePath,
        string metadataKey,
        string metadataValue,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default)
        => await ParquetFooterMetadata.WriteAsync(filePath, metadataKey, metadataValue, parquetOptions, cancellationToken).ConfigureAwait(false);
}
