using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Parquet;
using Parquet.Meta;

namespace Parquet.Query.Internal;

internal static class ParquetFooterMetadata
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static string Serialize<TModel>(TModel model)
    {
        ArgumentNullException.ThrowIfNull(model);

        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(model, JsonOptions);
        using var output = new MemoryStream();
        using (var compression = new BrotliStream(output, CompressionLevel.SmallestSize, leaveOpen: true))
        {
            compression.Write(jsonBytes, 0, jsonBytes.Length);
        }

        return Convert.ToBase64String(output.ToArray());
    }

    public static TModel? TryDeserialize<TModel>(string? payload)
    {
        if (string.IsNullOrWhiteSpace(payload))
        {
            return default;
        }

        try
        {
            var compressedBytes = Convert.FromBase64String(payload);
            using var input = new MemoryStream(compressedBytes, writable: false);
            using var compression = new BrotliStream(input, CompressionMode.Decompress);
            using var json = new MemoryStream();
            compression.CopyTo(json);
            return JsonSerializer.Deserialize<TModel>(json.ToArray(), JsonOptions);
        }
        catch (Exception ex) when (ex is FormatException or JsonException or InvalidDataException or InvalidOperationException)
        {
            return default;
        }
    }

    public static Task WriteAsync(
        string filePath,
        string metadataKey,
        string metadataValue,
        CancellationToken cancellationToken = default) =>
        WriteAsync(
            filePath,
            new[] { new KeyValuePair<string, string>(metadataKey, metadataValue) },
            cancellationToken);

    public static async Task WriteAsync(
        string filePath,
        IEnumerable<KeyValuePair<string, string>> metadata,
        CancellationToken cancellationToken = default)
    {
        var fileBytes = await System.IO.File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);

        using var input = new MemoryStream(fileBytes, writable: false);
        using var reader = await ParquetReader.CreateAsync(input, cancellationToken: cancellationToken).ConfigureAwait(false);

        FileMetaData footerMetadata = reader.Metadata!;
        footerMetadata.KeyValueMetadata = reader.CustomMetadata
            .Concat(metadata)
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
