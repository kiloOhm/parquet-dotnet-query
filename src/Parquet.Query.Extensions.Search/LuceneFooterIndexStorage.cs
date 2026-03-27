using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Parquet;
using Parquet.Meta;

namespace Parquet.Query.Extensions.Search;

internal static class LuceneFooterIndexStorage
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static string GetMetadataKey(string columnPath) =>
        LuceneIndexNames.MetadataPrefix + Uri.EscapeDataString(columnPath);

    public static string Serialize(LuceneFooterIndexModel index)
    {
        ArgumentNullException.ThrowIfNull(index);

        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(index, JsonOptions);
        using var output = new MemoryStream();
        using (var compression = new BrotliStream(output, CompressionLevel.SmallestSize, leaveOpen: true))
        {
            compression.Write(jsonBytes, 0, jsonBytes.Length);
        }

        return Convert.ToBase64String(output.ToArray());
    }

    public static LuceneFooterIndexModel? TryDeserialize(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return null;
        }

        try
        {
            var compressedBytes = Convert.FromBase64String(payload);
            using var input = new MemoryStream(compressedBytes, writable: false);
            using var compression = new BrotliStream(input, CompressionMode.Decompress);
            using var json = new MemoryStream();
            compression.CopyTo(json);
            return JsonSerializer.Deserialize<LuceneFooterIndexModel>(json.ToArray(), JsonOptions);
        }
        catch (FormatException)
        {
            return null;
        }
        catch (JsonException)
        {
            return null;
        }
        catch (InvalidDataException)
        {
            return null;
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }

    public static async Task WriteToFooterAsync(string filePath, string metadataKey, string metadataValue, CancellationToken cancellationToken = default)
    {
        var fileBytes = await System.IO.File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);

        using var input = new MemoryStream(fileBytes, writable: false);
        using var reader = await ParquetReader.CreateAsync(input, cancellationToken: cancellationToken).ConfigureAwait(false);

        FileMetaData footerMetadata = reader.Metadata!;
        footerMetadata.KeyValueMetadata = reader.CustomMetadata
            .Concat(new[] { new KeyValuePair<string, string>(metadataKey, metadataValue) })
            .GroupBy(entry => entry.Key, StringComparer.Ordinal)
            .Select(group => new KeyValue { Key = group.Key, Value = group.Last().Value })
            .ToList();

        var originalFooterLength = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
        var footerStart = fileBytes.Length - 8 - originalFooterLength;

        using var output = new MemoryStream();
        await output.WriteAsync(fileBytes.AsMemory(0, footerStart), cancellationToken).ConfigureAwait(false);

        var footerType = typeof(ParquetReader).Assembly.GetType("Parquet.File.ThriftFooter", throwOnError: true)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be loaded.");
        var footer = Activator.CreateInstance(footerType, footerMetadata)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be created.");
        var writeMethod = footerType.GetMethod(
            "Write",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic,
            binder: null,
            types: new[] { typeof(Stream) },
            modifiers: null)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter.Write(Stream) could not be found.");

        var newFooterLength = checked(Convert.ToInt32(writeMethod.Invoke(footer, new object[] { output })));
        await output.WriteAsync(BitConverter.GetBytes(newFooterLength), cancellationToken).ConfigureAwait(false);
        await output.WriteAsync(System.Text.Encoding.ASCII.GetBytes("PAR1"), cancellationToken).ConfigureAwait(false);
        await System.IO.File.WriteAllBytesAsync(filePath, output.ToArray(), cancellationToken).ConfigureAwait(false);
    }
}
