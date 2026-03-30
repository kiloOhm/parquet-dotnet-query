using Parquet.Query.Extensions.Search;
using Parquet.Query.Extensions.Writing.Attributes;

namespace Parquet.Query.Extensions.Writing.Attributes;

/// <summary>
/// Requests that the Lucene footer indexing strategy build a text-search index for the annotated string column.
/// </summary>
public sealed class ParquetLuceneIndexAttribute : ParquetExternalIndexAttribute
{
    /// <summary>
    /// Initializes a new Lucene index attribute.
    /// </summary>
    public ParquetLuceneIndexAttribute()
        : base(LuceneIndexNames.StrategyName)
    {
    }
}
