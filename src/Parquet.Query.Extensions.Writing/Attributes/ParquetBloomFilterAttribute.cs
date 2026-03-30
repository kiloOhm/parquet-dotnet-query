namespace Parquet.Query.Extensions.Writing.Attributes;

/// <summary>
/// Requests that parquet bloom filters be created for the annotated column.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
public sealed class ParquetBloomFilterAttribute : Attribute
{
}
