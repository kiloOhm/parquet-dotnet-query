namespace Parquet.Query.Extensions.Writing;

internal sealed class SerializerOptionsSnapshot
{
    private readonly QueryParquetSerializerOptions _options;

    private SerializerOptionsSnapshot(QueryParquetSerializerOptions options)
    {
        _options = options;
    }

    public bool Append => _options.Append;

    public Parquet.CompressionMethod CompressionMethod => _options.CompressionMethod;

    public System.IO.Compression.CompressionLevel CompressionLevel => _options.CompressionLevel;

    public int? RowGroupSize => _options.RowGroupSize;

    public bool PropertyNameCaseInsensitive => _options.PropertyNameCaseInsensitive;

#if PARQUET_V6
    public Parquet.ParquetOptions ParquetOptions => _options;
#else
    public Parquet.ParquetOptions? ParquetOptions => _options.ParquetOptions;
#endif

    public static SerializerOptionsSnapshot From(QueryParquetSerializerOptions options)
    {
        Guard.NotNull(options, nameof(options));
        return new SerializerOptionsSnapshot(CloneOptions(options));
    }

    public SerializerOptionsSnapshot WithOverrides(QueryParquetSerializerOptions options)
    {
        Guard.NotNull(options, nameof(options));
        return new SerializerOptionsSnapshot(CloneOptions(options));
    }

    public QueryParquetSerializerOptions ToSerializerOptions() => CloneOptions(_options);

    private static QueryParquetSerializerOptions CloneOptions(QueryParquetSerializerOptions source)
    {
#if PARQUET_V6
        var clone = new Parquet.ParquetOptions
        {
            Append = source.Append,
            CompressionMethod = source.CompressionMethod,
            CompressionLevel = source.CompressionLevel,
            RowGroupSize = source.RowGroupSize,
            PropertyNameCaseInsensitive = source.PropertyNameCaseInsensitive,
            TreatByteArrayAsString = source.TreatByteArrayAsString,
            TreatBigIntegersAsDates = source.TreatBigIntegersAsDates,
            UseDateOnlyTypeForDates = source.UseDateOnlyTypeForDates,
            DictionaryEncodingThreshold = source.DictionaryEncodingThreshold,
            DictionaryEncodingSampleSize = source.DictionaryEncodingSampleSize,
            DataPageRowCountLimit = source.DataPageRowCountLimit,
            MaximumSmallPoolFreeBytes = source.MaximumSmallPoolFreeBytes,
            MaximumLargePoolFreeBytes = source.MaximumLargePoolFreeBytes,
            UseBigDecimal = source.UseBigDecimal,
            Encryption = source.Encryption,
            Decryption = source.Decryption
        };

        foreach (var entry in source.ColumnEncodingHints)
        {
            clone.ColumnEncodingHints[entry.Key] = entry.Value;
        }

        foreach (var entry in source.BloomFilterOptionsByColumn)
        {
            clone.BloomFilterOptionsByColumn[entry.Key] = new Parquet.ParquetOptions.BloomFilterOptions
            {
                EnableBloomFilters = entry.Value.EnableBloomFilters,
                BloomFilterFpp = entry.Value.BloomFilterFpp,
                BloomFilterBitsPerValueOverride = entry.Value.BloomFilterBitsPerValueOverride
            };
        }

        return clone;
#else
        return new Parquet.Serialization.ParquetSerializerOptions
        {
            Append = source.Append,
            CompressionMethod = source.CompressionMethod,
            CompressionLevel = source.CompressionLevel,
            RowGroupSize = source.RowGroupSize,
            PropertyNameCaseInsensitive = source.PropertyNameCaseInsensitive,
            ParquetOptions = CloneParquetOptions(source.ParquetOptions)
        };
#endif
    }

#if !PARQUET_V6
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

        foreach (var entry in source.BloomFilterOptionsByColumn)
        {
            clone.BloomFilterOptionsByColumn[entry.Key] = new Parquet.ParquetOptions.BloomFilterOptions
            {
                EnableBloomFilters = entry.Value.EnableBloomFilters,
                BloomFilterFpp = entry.Value.BloomFilterFpp,
                BloomFilterBitsPerValueOverride = entry.Value.BloomFilterBitsPerValueOverride
            };
        }

        foreach (var entry in source.ColumnKeys)
        {
            clone.ColumnKeys[entry.Key] = new Parquet.ParquetOptions.ColumnKeySpec(
                entry.Value.Key,
                entry.Value.KeyMetadata?.ToArray());
        }

        return clone;
    }
#endif
}
