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
            ParquetOptions = ParquetOptions
        };
}
