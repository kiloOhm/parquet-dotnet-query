namespace Parquet.Query.Internal;

internal static class ParquetOptionsFactory
{
    public static ParquetOptions Clone(ParquetOptions? source)
    {
        var clone = new ParquetOptions();
        if (source is null)
        {
            return clone;
        }

        clone.TreatByteArrayAsString = source.TreatByteArrayAsString;
        clone.TreatBigIntegersAsDates = source.TreatBigIntegersAsDates;
#if NET6_0_OR_GREATER
        clone.UseDateOnlyTypeForDates = source.UseDateOnlyTypeForDates;
        clone.UseTimeOnlyTypeForTimeMillis = source.UseTimeOnlyTypeForTimeMillis;
        clone.UseTimeOnlyTypeForTimeMicros = source.UseTimeOnlyTypeForTimeMicros;
#endif
        clone.UseDictionaryEncoding = source.UseDictionaryEncoding;
        clone.DictionaryEncodingThreshold = source.DictionaryEncodingThreshold;
        clone.UseDeltaBinaryPackedEncoding = source.UseDeltaBinaryPackedEncoding;
        clone.MaximumSmallPoolFreeBytes = source.MaximumSmallPoolFreeBytes;
        clone.MaximumLargePoolFreeBytes = source.MaximumLargePoolFreeBytes;
        clone.UseBigDecimal = source.UseBigDecimal;

        foreach (var kvp in source.BloomFilterOptionsByColumn)
        {
            clone.BloomFilterOptionsByColumn[kvp.Key] = new ParquetOptions.BloomFilterOptions
            {
                EnableBloomFilters = kvp.Value.EnableBloomFilters,
                BloomFilterFpp = kvp.Value.BloomFilterFpp,
                BloomFilterBitsPerValueOverride = kvp.Value.BloomFilterBitsPerValueOverride
            };
        }

        clone.UsePlaintextFooter = source.UsePlaintextFooter;
        clone.FooterEncryptionKey = source.FooterEncryptionKey;
        clone.FooterEncryptionKeyMetadata = source.FooterEncryptionKeyMetadata?.ToArray();
        clone.FooterSigningKey = source.FooterSigningKey;
        clone.FooterSigningKeyMetadata = source.FooterSigningKeyMetadata?.ToArray();
        clone.AADPrefix = source.AADPrefix;
        clone.SupplyAadPrefix = source.SupplyAadPrefix;
        clone.UseCtrVariant = source.UseCtrVariant;

        foreach (var kvp in source.ColumnKeys)
        {
            clone.ColumnKeys[kvp.Key] = new ParquetOptions.ColumnKeySpec(
                kvp.Value.Key,
                kvp.Value.KeyMetadata?.ToArray());
        }

        clone.ColumnKeyResolver = source.ColumnKeyResolver;

        return clone;
    }
}
