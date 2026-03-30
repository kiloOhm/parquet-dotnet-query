using System.IO.Compression;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Parquet;
using Parquet.Meta;
#if NET48
using BrotliStream = BrotliSharpLib.BrotliStream;
#endif

namespace Parquet.Query.Internal;

internal static class ParquetFooterMetadata
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    public static string Serialize<TModel>(TModel model)
    {
        Guard.NotNull(model, nameof(model));

        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(model, JsonOptions);
        using var output = new MemoryStream();
        using (var compression =
#if NET48
            new BrotliStream(output, CompressionMode.Compress, leaveOpen: true)
#else
            new BrotliStream(output, CompressionLevel.SmallestSize, leaveOpen: true)
#endif
            )
        {
#if NET48
            compression.SetQuality(11);
#endif
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
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default) =>
        WriteAsync(
            filePath,
            new[] { new KeyValuePair<string, string>(metadataKey, metadataValue) },
            parquetOptions,
            cancellationToken);

    public static async Task WriteAsync(
        string filePath,
        IEnumerable<KeyValuePair<string, string>> metadata,
        ParquetOptions? parquetOptions = null,
        CancellationToken cancellationToken = default)
    {
        Guard.NotNullOrWhiteSpace(filePath, nameof(filePath));
        Guard.NotNull(metadata, nameof(metadata));

        var fileBytes = await AsyncCompatibility.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);
        var readOptions = ParquetOptionsFactory.Clone(parquetOptions);

        FileMetaData footerMetadata;
        using (var input = new MemoryStream(fileBytes, writable: false))
        using (var reader = await ParquetReader.CreateAsync(
            input,
            readOptions,
            leaveStreamOpen: false,
            cancellationToken: cancellationToken).ConfigureAwait(false))
        {
            footerMetadata = reader.Metadata!;
            footerMetadata.KeyValueMetadata = reader.CustomMetadata
                .Concat(metadata)
                .GroupBy(entry => entry.Key, StringComparer.Ordinal)
                .Select(group => new KeyValue { Key = group.Key, Value = group.Last().Value })
                .ToList();
        }

        var footerBytes = SerializeFooter(footerMetadata);
        var originalFooterLength = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
        var footerStart = fileBytes.Length - 8 - originalFooterLength;
        var magic = System.Text.Encoding.ASCII.GetString(fileBytes, fileBytes.Length - 4, 4);

        using var output = new MemoryStream();
        await AsyncCompatibility.WriteAsync(output, fileBytes, 0, footerStart, cancellationToken).ConfigureAwait(false);

        if (string.Equals(magic, "PARE", StringComparison.Ordinal))
        {
            WriteEncryptedFooter(output, fileBytes, footerStart, footerBytes, parquetOptions);
        }
        else if (string.Equals(magic, "PAR1", StringComparison.Ordinal))
        {
            if (footerMetadata.EncryptionAlgorithm is not null)
            {
                WriteSignedPlaintextFooter(output, footerBytes, footerMetadata.EncryptionAlgorithm, parquetOptions);
            }
            else
            {
                WritePlaintextFooter(output, footerBytes);
            }
        }
        else
        {
            throw new InvalidDataException($"Unsupported parquet footer magic '{magic}'.");
        }

        await AsyncCompatibility.WriteAllBytesAsync(filePath, output.ToArray(), cancellationToken).ConfigureAwait(false);
    }

    private static byte[] SerializeFooter(FileMetaData footerMetadata)
    {
        var footerType = typeof(ParquetReader).Assembly.GetType("Parquet.File.ThriftFooter", throwOnError: true)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be loaded.");
        var footer = Activator.CreateInstance(footerType, footerMetadata)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be created.");
        var writeMethod = footerType.GetMethod(
            "Write",
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            binder: null,
            types: new[] { typeof(Stream) },
            modifiers: null)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter.Write(Stream) could not be found.");

        using var output = new MemoryStream();
        _ = writeMethod.Invoke(footer, new object[] { output });
        return output.ToArray();
    }

    private static void WritePlaintextFooter(Stream output, byte[] footerBytes)
    {
        output.Write(footerBytes, 0, footerBytes.Length);
        output.Write(BitConverter.GetBytes(footerBytes.Length), 0, sizeof(int));
        output.Write(System.Text.Encoding.ASCII.GetBytes("PAR1"), 0, 4);
    }

    private static void WriteEncryptedFooter(
        Stream output,
        byte[] fileBytes,
        int footerStart,
        byte[] footerBytes,
        ParquetOptions? parquetOptions)
    {
        if (parquetOptions is null || string.IsNullOrWhiteSpace(parquetOptions.FooterEncryptionKey))
        {
            throw new InvalidDataException($"{nameof(ParquetOptions.FooterEncryptionKey)} is required for files with encrypted footers.");
        }

        var footerEncryptionKey = parquetOptions.FooterEncryptionKey;

        var tailLength = fileBytes.Length - 8 - footerStart;
        var encryptionBaseType = typeof(ParquetReader).Assembly.GetType("Parquet.Encryption.EncryptionBase", throwOnError: true)
            ?? throw new InvalidOperationException("Parquet.Encryption.EncryptionBase could not be loaded.");
        var protocolReaderType = typeof(ParquetReader).Assembly.GetType("Parquet.Meta.Proto.ThriftCompactProtocolReader", throwOnError: true)
            ?? throw new InvalidOperationException("Parquet.Meta.Proto.ThriftCompactProtocolReader could not be loaded.");
        using var tailStream = new MemoryStream(fileBytes, footerStart, tailLength, writable: false);
        var protocolReader = Activator.CreateInstance(protocolReaderType, tailStream)
            ?? throw new InvalidOperationException("Parquet.Meta.Proto.ThriftCompactProtocolReader could not be created.");
        var createFromCryptoMeta = encryptionBaseType.GetMethod(
            "CreateFromCryptoMeta",
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            types: new[] { protocolReaderType, typeof(string), typeof(string) },
            modifiers: null)
            ?? throw new InvalidOperationException("EncryptionBase.CreateFromCryptoMeta could not be found.");
        var encrypter = createFromCryptoMeta.Invoke(
            obj: null,
            parameters: new object?[] { protocolReader, footerEncryptionKey, parquetOptions.AADPrefix })
            ?? throw new InvalidOperationException("EncryptionBase.CreateFromCryptoMeta returned null.");

        var cryptoMetadataLength = checked((int)tailStream.Position);
        var encryptFooterMethod = encrypter.GetType().GetMethod(
            "EncryptFooter",
            BindingFlags.Public | BindingFlags.Instance,
            binder: null,
            types: new[] { typeof(byte[]) },
            modifiers: null)
            ?? throw new InvalidOperationException("EncryptionBase.EncryptFooter(byte[]) could not be found.");
        var encryptedFooter = (byte[]?)encryptFooterMethod.Invoke(encrypter, new object[] { footerBytes })
            ?? throw new InvalidOperationException("EncryptionBase.EncryptFooter(byte[]) returned null.");

        output.Write(fileBytes, footerStart, cryptoMetadataLength);
        output.Write(encryptedFooter, 0, encryptedFooter.Length);
        output.Write(BitConverter.GetBytes(cryptoMetadataLength + encryptedFooter.Length), 0, sizeof(int));
        output.Write(System.Text.Encoding.ASCII.GetBytes("PARE"), 0, 4);
    }

    private static void WriteSignedPlaintextFooter(
        Stream output,
        byte[] footerBytes,
        EncryptionAlgorithm algorithm,
        ParquetOptions? parquetOptions)
    {
#if NET48
        throw new PlatformNotSupportedException("Signed plaintext footer metadata rewrites require AES-GCM, which is not available on .NET Framework 4.8.");
#else
        if (parquetOptions is null || string.IsNullOrWhiteSpace(parquetOptions.FooterSigningKey))
        {
            throw new InvalidDataException($"{nameof(ParquetOptions.FooterSigningKey)} is required to rewrite signed plaintext footers.");
        }

        var footerSigningKey = parquetOptions.FooterSigningKey;

        var encryptionBaseType = typeof(ParquetReader).Assembly.GetType("Parquet.Encryption.EncryptionBase", throwOnError: true)
            ?? throw new InvalidOperationException("Parquet.Encryption.EncryptionBase could not be loaded.");
        var createFromAlgorithm = encryptionBaseType.GetMethod(
            "CreateFromAlgorithm",
            BindingFlags.Public | BindingFlags.Static,
            binder: null,
            types: new[] { typeof(EncryptionAlgorithm), typeof(string), typeof(string) },
            modifiers: null)
            ?? throw new InvalidOperationException("EncryptionBase.CreateFromAlgorithm could not be found.");
        var signer = createFromAlgorithm.Invoke(
            obj: null,
            parameters: new object?[] { algorithm, footerSigningKey, parquetOptions.AADPrefix })
            ?? throw new InvalidOperationException("EncryptionBase.CreateFromAlgorithm returned null.");

        var buildAadMethod = encryptionBaseType
            .GetMethods(BindingFlags.Instance | BindingFlags.NonPublic)
            .FirstOrDefault(method => string.Equals(method.Name, "BuildAad", StringComparison.Ordinal))
            ?? throw new InvalidOperationException("EncryptionBase.BuildAad could not be found.");
        var aad = (byte[]?)buildAadMethod.Invoke(signer, new object?[] { ParquetModules.Footer, null, null, null })
            ?? throw new InvalidOperationException("EncryptionBase.BuildAad returned null.");

        var footerKeyProperty = encryptionBaseType.GetProperty(
            "FooterEncryptionKey",
            BindingFlags.Instance | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("EncryptionBase.FooterEncryptionKey could not be found.");
        var footerSigningKeyBytes = (byte[]?)footerKeyProperty.GetValue(signer)
            ?? throw new InvalidOperationException("EncryptionBase.FooterEncryptionKey returned null.");

        var nonce = new byte[12];
#if NET6_0_OR_GREATER
        RandomNumberGenerator.Fill(nonce);
#else
        using (var random = RandomNumberGenerator.Create())
        {
            random.GetBytes(nonce);
        }
#endif

        var tag = ComputeGcmTag(footerSigningKeyBytes, nonce, footerBytes, aad);
        output.Write(footerBytes, 0, footerBytes.Length);
        output.Write(nonce, 0, nonce.Length);
        output.Write(tag, 0, tag.Length);
        output.Write(BitConverter.GetBytes(footerBytes.Length + nonce.Length + tag.Length), 0, sizeof(int));
        output.Write(System.Text.Encoding.ASCII.GetBytes("PAR1"), 0, 4);
#endif
    }

    private static byte[] ComputeGcmTag(byte[] key, byte[] nonce, byte[] plaintext, byte[] aad)
    {
        var ciphertext = new byte[plaintext.Length];
        var tag = new byte[16];
#if NET8_0_OR_GREATER
        using var gcm = new AesGcm(key, tag.Length);
        gcm.Encrypt(nonce, plaintext, ciphertext, tag, aad);
        return tag;
#elif NET7_0_OR_GREATER || NET6_0_OR_GREATER || NET5_0_OR_GREATER || NETCOREAPP3_0_OR_GREATER || NETSTANDARD2_1
        using var gcm = new AesGcm(key);
        gcm.Encrypt(nonce, plaintext, ciphertext, tag, aad);
        return tag;
#else
        throw new PlatformNotSupportedException("AES-GCM is not available on this target.");
#endif
    }
}
