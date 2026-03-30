using System.IO.Compression;
using Parquet;

namespace Parquet.Query.Extensions.Writing.Attributes;

/// <summary>
/// Defines default serialization options for a row type when it is written to parquet.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class ParquetWriteOptionsAttribute : Attribute
{
    /// <summary>
    /// Gets or sets the compression method to use when writing.
    /// </summary>
    public CompressionMethod CompressionMethod { get; set; } = CompressionMethod.Snappy;

    /// <summary>
    /// Gets or sets the compression level to use when writing.
    /// </summary>
    public CompressionLevel CompressionLevel { get; set; } = CompressionLevel.Optimal;

    /// <summary>
    /// Gets or sets the target row-group size. A value of <c>0</c> keeps the serializer default.
    /// </summary>
    public int RowGroupSize { get; set; }
}
