using Parquet.Query.Extensions.Indexing;
using Parquet.Query.Extensions.Writing.Attributes;

namespace Parquet.Query.Extensions.Writing.Attributes;

/// <summary>
/// Requests that the footer bitmap indexing strategy build a low-cardinality equality index for the annotated string column.
/// </summary>
public sealed class ParquetFooterBitmapIndexAttribute : ParquetExternalIndexAttribute
{
    /// <summary>
    /// Initializes a new footer bitmap index attribute.
    /// </summary>
    public ParquetFooterBitmapIndexAttribute()
        : base(FooterIndexNames.BitmapStrategyName)
    {
    }
}
