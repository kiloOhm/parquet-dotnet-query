using Parquet.Query.Internal;
using Parquet;

namespace Parquet.Query.Extensions.Indexing;

internal static class FooterIndexStorage
{
    public static string GetHashMetadataKey(string columnPath) =>
        FooterIndexNames.HashMetadataPrefix + Uri.EscapeDataString(columnPath);

    public static string GetBitmapMetadataKey(string columnPath) =>
        FooterIndexNames.BitmapMetadataPrefix + Uri.EscapeDataString(columnPath);

    public static string GetSortOrderMetadataKey() =>
        FooterIndexNames.SortOrderMetadataKey;

    public static string Serialize<TModel>(TModel index)
        => ParquetFooterMetadata.Serialize(index);

    public static TModel? TryDeserialize<TModel>(string? payload)
        => ParquetFooterMetadata.TryDeserialize<TModel>(payload);

    public static async Task WriteToFooterAsync(
        string filePath,
        string metadataKey,
        string metadataValue,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default)
        => await ParquetFooterMetadata.WriteAsync(filePath, metadataKey, metadataValue, parquetOptions, cancellationToken).ConfigureAwait(false);
}
