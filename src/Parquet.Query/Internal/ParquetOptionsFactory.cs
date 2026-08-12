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
#if PARQUET_V6
        clone.UseDateOnlyTypeForDates = source.UseDateOnlyTypeForDates;
        clone.DictionaryEncodingThreshold = source.DictionaryEncodingThreshold;
        clone.DictionaryEncodingSampleSize = source.DictionaryEncodingSampleSize;
        clone.DataPageRowCountLimit = source.DataPageRowCountLimit;
        clone.CompressionMethod = source.CompressionMethod;
        clone.CompressionLevel = source.CompressionLevel;
        clone.Append = source.Append;
        clone.RowGroupSize = source.RowGroupSize;
        clone.PropertyNameCaseInsensitive = source.PropertyNameCaseInsensitive;
        clone.Encryption = CloneEncryption(source.Encryption);
        clone.Decryption = CloneDecryption(source.Decryption);

        foreach (var entry in source.ColumnEncodingHints)
        {
            clone.ColumnEncodingHints[entry.Key] = entry.Value;
        }
#else
#if NET6_0_OR_GREATER
        clone.UseDateOnlyTypeForDates = source.UseDateOnlyTypeForDates;
        clone.UseTimeOnlyTypeForTimeMillis = source.UseTimeOnlyTypeForTimeMillis;
        clone.UseTimeOnlyTypeForTimeMicros = source.UseTimeOnlyTypeForTimeMicros;
#endif
        clone.UseDictionaryEncoding = source.UseDictionaryEncoding;
        clone.DictionaryEncodingThreshold = source.DictionaryEncodingThreshold;
        clone.UseDeltaBinaryPackedEncoding = source.UseDeltaBinaryPackedEncoding;
#endif
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

#if !PARQUET_V6
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
#endif

        return clone;
    }

#if PARQUET_V6
    private static ParquetEncryptionOptions? CloneEncryption(ParquetEncryptionOptions? source)
    {
        if (source is null)
        {
            return null;
        }

        var clone = new ParquetEncryptionOptions(new ParquetKey(
            source.FooterKey.KeyBytes,
            source.FooterKey.KeyMetadata ?? Array.Empty<byte>()))
        {
            Algorithm = source.Algorithm,
            EncryptFooter = source.EncryptFooter,
            EncryptAllColumns = source.EncryptAllColumns,
            AadPrefix = source.AadPrefix?.ToArray(),
            StoreAadPrefix = source.StoreAadPrefix
        };
        foreach (var path in source.FooterKeyColumns)
        {
            clone.FooterKeyColumns.Add(path);
        }

        foreach (var entry in source.ColumnKeys)
        {
            clone.ColumnKeys[entry.Key] = new ParquetKey(
                entry.Value.KeyBytes,
                entry.Value.KeyMetadata ?? Array.Empty<byte>());
        }

        return clone;
    }

    private static ParquetDecryptionOptions? CloneDecryption(ParquetDecryptionOptions? source)
    {
        if (source is null)
        {
            return null;
        }

        var clone = new ParquetDecryptionOptions
        {
            FooterKey = source.FooterKey?.ToArray(),
            AadPrefix = source.AadPrefix?.ToArray(),
            KeyRetriever = source.KeyRetriever
        };
        foreach (var entry in source.ColumnKeys)
        {
            clone.ColumnKeys[entry.Key] = entry.Value.ToArray();
        }

        return clone;
    }
#endif
}
