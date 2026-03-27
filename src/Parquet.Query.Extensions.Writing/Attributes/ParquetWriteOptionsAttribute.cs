using System.IO.Compression;
using Parquet;

namespace Parquet.Query.Extensions.Writing.Attributes;

[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class ParquetWriteOptionsAttribute : Attribute
{
    public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;

    public int RowGroupSize { get; set; }
}
