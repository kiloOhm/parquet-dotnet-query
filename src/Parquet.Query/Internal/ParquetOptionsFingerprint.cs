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
        Append(builder, nameof(options.UseTimeOnlyTypeForTimeMillis), options.UseTimeOnlyTypeForTimeMillis);
        Append(builder, nameof(options.UseTimeOnlyTypeForTimeMicros), options.UseTimeOnlyTypeForTimeMicros);
        Append(builder, nameof(options.UseDictionaryEncoding), options.UseDictionaryEncoding);
        Append(builder, nameof(options.DictionaryEncodingThreshold), options.DictionaryEncodingThreshold);
        Append(builder, nameof(options.UseDeltaBinaryPackedEncoding), options.UseDeltaBinaryPackedEncoding);
        Append(builder, nameof(options.MaximumSmallPoolFreeBytes), options.MaximumSmallPoolFreeBytes);
        Append(builder, nameof(options.MaximumLargePoolFreeBytes), options.MaximumLargePoolFreeBytes);
        Append(builder, nameof(options.UseBigDecimal), options.UseBigDecimal);
        Append(builder, nameof(options.UsePlaintextFooter), options.UsePlaintextFooter);
        Append(builder, nameof(options.FooterEncryptionKey), options.FooterEncryptionKey);
        Append(builder, nameof(options.FooterEncryptionKeyMetadata), HexEncoding.ToHexString(options.FooterEncryptionKeyMetadata ?? Array.Empty<byte>()));
        Append(builder, nameof(options.FooterSigningKey), options.FooterSigningKey);
        Append(builder, nameof(options.FooterSigningKeyMetadata), HexEncoding.ToHexString(options.FooterSigningKeyMetadata ?? Array.Empty<byte>()));
        Append(builder, nameof(options.AADPrefix), options.AADPrefix);
        Append(builder, nameof(options.SupplyAadPrefix), options.SupplyAadPrefix);
        Append(builder, nameof(options.UseCtrVariant), options.UseCtrVariant);

        foreach (var entry in options.BloomFilterOptionsByColumn.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            Append(builder, $"bloom:{entry.Key}", $"{entry.Value.EnableBloomFilters}|{entry.Value.BloomFilterFpp}|{entry.Value.BloomFilterBitsPerValueOverride}");
        }

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

        return builder.ToString();
    }

    private static void Append(StringBuilder builder, string name, object? value)
    {
        builder.Append(name);
        builder.Append('=');
        builder.Append(value);
        builder.Append(';');
    }
}
