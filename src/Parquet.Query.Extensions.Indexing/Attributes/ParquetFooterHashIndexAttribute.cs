using Parquet.Query.Extensions.Indexing;
using Parquet.Query.Extensions.Writing.Attributes;

namespace Parquet.Query.Extensions.Writing.Attributes;

/// <summary>
/// Requests that the footer hash indexing strategy build a lookup index for the annotated string column.
/// </summary>
public sealed class ParquetFooterHashIndexAttribute : ParquetExternalIndexAttribute
{
    /// <summary>
    /// Initializes a new footer hash index attribute.
    /// </summary>
    public ParquetFooterHashIndexAttribute()
        : base(FooterIndexNames.HashStrategyName)
    {
    }
}
