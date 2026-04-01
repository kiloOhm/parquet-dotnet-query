using Parquet.Serialization;

namespace Parquet.Query.Extensions.Writing;

internal sealed class SerializerOptionsSnapshot
{
    private SerializerOptionsSnapshot(
        bool append,
        Parquet.CompressionMethod compressionMethod,
        System.IO.Compression.CompressionLevel compressionLevel,
        int? rowGroupSize,
        bool propertyNameCaseInsensitive,
        Parquet.ParquetOptions? parquetOptions)
    {
        Append = append;
        CompressionMethod = compressionMethod;
        CompressionLevel = compressionLevel;
        RowGroupSize = rowGroupSize;
        PropertyNameCaseInsensitive = propertyNameCaseInsensitive;
        ParquetOptions = parquetOptions;
    }

    public bool Append { get; }

    public Parquet.CompressionMethod CompressionMethod { get; }

    public System.IO.Compression.CompressionLevel CompressionLevel { get; }

    public int? RowGroupSize { get; }

    public bool PropertyNameCaseInsensitive { get; }

    public Parquet.ParquetOptions? ParquetOptions { get; }

    public static SerializerOptionsSnapshot From(ParquetSerializerOptions options)
    {
        Guard.NotNull(options, nameof(options));
        return new SerializerOptionsSnapshot(
            options.Append,
            options.CompressionMethod,
            options.CompressionLevel,
            options.RowGroupSize,
            options.PropertyNameCaseInsensitive,
            options.ParquetOptions);
    }

    public SerializerOptionsSnapshot WithOverrides(ParquetSerializerOptions options)
    {
        Guard.NotNull(options, nameof(options));
        return new SerializerOptionsSnapshot(
            options.Append,
            options.CompressionMethod,
            options.CompressionLevel,
            options.RowGroupSize,
            options.PropertyNameCaseInsensitive,
            options.ParquetOptions);
    }

    public ParquetSerializerOptions ToSerializerOptions() =>
        new()
        {
            Append = Append,
            CompressionMethod = CompressionMethod,
            CompressionLevel = CompressionLevel,
            RowGroupSize = RowGroupSize,
            PropertyNameCaseInsensitive = PropertyNameCaseInsensitive,
            ParquetOptions = CloneParquetOptions(ParquetOptions)
        };

    private static Parquet.ParquetOptions? CloneParquetOptions(Parquet.ParquetOptions? source)
    {
        if (source is null)
        {
            return null;
        }

        var clone = new Parquet.ParquetOptions
        {
            TreatByteArrayAsString = source.TreatByteArrayAsString,
            TreatBigIntegersAsDates = source.TreatBigIntegersAsDates,
#if NET6_0_OR_GREATER || NET48
            UseDateOnlyTypeForDates = source.UseDateOnlyTypeForDates,
            UseTimeOnlyTypeForTimeMillis = source.UseTimeOnlyTypeForTimeMillis,
            UseTimeOnlyTypeForTimeMicros = source.UseTimeOnlyTypeForTimeMicros,
#endif
            UseDictionaryEncoding = source.UseDictionaryEncoding,
            DictionaryEncodingThreshold = source.DictionaryEncodingThreshold,
            UseDeltaBinaryPackedEncoding = source.UseDeltaBinaryPackedEncoding,
            DataPageRowCountLimit = source.DataPageRowCountLimit,
            MaximumSmallPoolFreeBytes = source.MaximumSmallPoolFreeBytes,
            MaximumLargePoolFreeBytes = source.MaximumLargePoolFreeBytes,
            UseBigDecimal = source.UseBigDecimal,
            UsePlaintextFooter = source.UsePlaintextFooter,
            FooterEncryptionKey = source.FooterEncryptionKey,
            FooterEncryptionKeyMetadata = source.FooterEncryptionKeyMetadata?.ToArray(),
            FooterSigningKey = source.FooterSigningKey,
            FooterSigningKeyMetadata = source.FooterSigningKeyMetadata?.ToArray(),
            AADPrefix = source.AADPrefix,
            SupplyAadPrefix = source.SupplyAadPrefix,
            UseCtrVariant = source.UseCtrVariant,
            ColumnKeyResolver = source.ColumnKeyResolver
        };

        foreach (var kvp in source.BloomFilterOptionsByColumn)
        {
            clone.BloomFilterOptionsByColumn[kvp.Key] = new Parquet.ParquetOptions.BloomFilterOptions
            {
                EnableBloomFilters = kvp.Value.EnableBloomFilters,
                BloomFilterFpp = kvp.Value.BloomFilterFpp,
                BloomFilterBitsPerValueOverride = kvp.Value.BloomFilterBitsPerValueOverride
            };
        }

        foreach (var kvp in source.ColumnKeys)
        {
            clone.ColumnKeys[kvp.Key] = new Parquet.ParquetOptions.ColumnKeySpec(
                kvp.Value.Key,
                kvp.Value.KeyMetadata?.ToArray());
        }

        return clone;
    }
}
