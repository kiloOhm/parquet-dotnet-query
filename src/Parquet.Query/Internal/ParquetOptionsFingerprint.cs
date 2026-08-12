using System.Runtime.CompilerServices;
using System.Text;
using Parquet;

namespace Parquet.Query.Internal;

internal static class ParquetOptionsFingerprint
{
    public static string Create(ParquetOptions? options)
    {
        if (options is null)
        {
            return "default";
        }

        var builder = new StringBuilder();
        Append(builder, nameof(options.TreatByteArrayAsString), options.TreatByteArrayAsString);
        Append(builder, nameof(options.TreatBigIntegersAsDates), options.TreatBigIntegersAsDates);
        Append(builder, nameof(options.UseDateOnlyTypeForDates), options.UseDateOnlyTypeForDates);
#if PARQUET_V6
        Append(builder, nameof(options.DictionaryEncodingThreshold), options.DictionaryEncodingThreshold);
        Append(builder, nameof(options.DictionaryEncodingSampleSize), options.DictionaryEncodingSampleSize);
        Append(builder, nameof(options.DataPageRowCountLimit), options.DataPageRowCountLimit);
        Append(builder, nameof(options.CompressionMethod), options.CompressionMethod);
        Append(builder, nameof(options.CompressionLevel), options.CompressionLevel);
        AppendEncryption(builder, options.Encryption);
        AppendDecryption(builder, options.Decryption);
        foreach (var entry in options.ColumnEncodingHints.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            Append(builder, $"encoding:{entry.Key}", entry.Value);
        }
#else
        Append(builder, nameof(options.UseTimeOnlyTypeForTimeMillis), options.UseTimeOnlyTypeForTimeMillis);
        Append(builder, nameof(options.UseTimeOnlyTypeForTimeMicros), options.UseTimeOnlyTypeForTimeMicros);
        Append(builder, nameof(options.UseDictionaryEncoding), options.UseDictionaryEncoding);
        Append(builder, nameof(options.DictionaryEncodingThreshold), options.DictionaryEncodingThreshold);
        Append(builder, nameof(options.UseDeltaBinaryPackedEncoding), options.UseDeltaBinaryPackedEncoding);
        Append(builder, nameof(options.UsePlaintextFooter), options.UsePlaintextFooter);
        Append(builder, nameof(options.FooterEncryptionKey), options.FooterEncryptionKey);
        Append(builder, nameof(options.FooterEncryptionKeyMetadata), HexEncoding.ToHexString(options.FooterEncryptionKeyMetadata ?? Array.Empty<byte>()));
        Append(builder, nameof(options.FooterSigningKey), options.FooterSigningKey);
        Append(builder, nameof(options.FooterSigningKeyMetadata), HexEncoding.ToHexString(options.FooterSigningKeyMetadata ?? Array.Empty<byte>()));
        Append(builder, nameof(options.AADPrefix), options.AADPrefix);
        Append(builder, nameof(options.SupplyAadPrefix), options.SupplyAadPrefix);
        Append(builder, nameof(options.UseCtrVariant), options.UseCtrVariant);

        foreach (var entry in options.ColumnKeys.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            Append(builder, $"column:{entry.Key}", $"{entry.Value.Key}|{HexEncoding.ToHexString(entry.Value.KeyMetadata ?? Array.Empty<byte>())}");
        }

        if (options.ColumnKeyResolver is not null)
        {
            var method = options.ColumnKeyResolver.Method;
            Append(
                builder,
                nameof(options.ColumnKeyResolver),
                $"{method.Module.ModuleVersionId}:{method.MetadataToken}:{RuntimeHelpers.GetHashCode(options.ColumnKeyResolver.Target ?? options.ColumnKeyResolver)}");
        }
#endif

        Append(builder, nameof(options.MaximumSmallPoolFreeBytes), options.MaximumSmallPoolFreeBytes);
        Append(builder, nameof(options.MaximumLargePoolFreeBytes), options.MaximumLargePoolFreeBytes);
        Append(builder, nameof(options.UseBigDecimal), options.UseBigDecimal);

        foreach (var entry in options.BloomFilterOptionsByColumn.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            Append(builder, $"bloom:{entry.Key}", $"{entry.Value.EnableBloomFilters}|{entry.Value.BloomFilterFpp}|{entry.Value.BloomFilterBitsPerValueOverride}");
        }

        return builder.ToString();
    }

#if PARQUET_V6
    private static void AppendEncryption(StringBuilder builder, ParquetEncryptionOptions? options)
    {
        if (options is null)
        {
            Append(builder, "encryption", null);
            return;
        }

        Append(builder, "encryption:footerKey", HexEncoding.ToHexString(options.FooterKey.KeyBytes));
        Append(builder, "encryption:footerMetadata", HexEncoding.ToHexString(options.FooterKey.KeyMetadata ?? Array.Empty<byte>()));
        Append(builder, "encryption:algorithm", options.Algorithm);
        Append(builder, "encryption:encryptFooter", options.EncryptFooter);
        Append(builder, "encryption:encryptAllColumns", options.EncryptAllColumns);
        Append(builder, "encryption:aad", HexEncoding.ToHexString(options.AadPrefix ?? Array.Empty<byte>()));
        Append(builder, "encryption:storeAad", options.StoreAadPrefix);
        foreach (var path in options.FooterKeyColumns.OrderBy(path => path, StringComparer.Ordinal))
        {
            Append(builder, $"encryption:footerColumn:{path}", true);
        }

        foreach (var entry in options.ColumnKeys.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            Append(builder, $"encryption:column:{entry.Key}",
                $"{HexEncoding.ToHexString(entry.Value.KeyBytes)}|{HexEncoding.ToHexString(entry.Value.KeyMetadata ?? Array.Empty<byte>())}");
        }
    }

    private static void AppendDecryption(StringBuilder builder, ParquetDecryptionOptions? options)
    {
        if (options is null)
        {
            Append(builder, "decryption", null);
            return;
        }

        Append(builder, "decryption:footerKey", HexEncoding.ToHexString(options.FooterKey ?? Array.Empty<byte>()));
        Append(builder, "decryption:aad", HexEncoding.ToHexString(options.AadPrefix ?? Array.Empty<byte>()));
        foreach (var entry in options.ColumnKeys.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            Append(builder, $"decryption:column:{entry.Key}", HexEncoding.ToHexString(entry.Value));
        }

        if (options.KeyRetriever is not null)
        {
            Append(builder, "decryption:keyRetriever", RuntimeHelpers.GetHashCode(options.KeyRetriever));
        }
    }
#endif

    private static void Append(StringBuilder builder, string name, object? value)
    {
        builder.Append(name);
        builder.Append('=');
        builder.Append(value);
        builder.Append(';');
    }
}
