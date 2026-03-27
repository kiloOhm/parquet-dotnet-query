namespace Parquet.Query.Extensions.Writing.Attributes;

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
public sealed class ParquetBloomFilterAttribute : Attribute
{
}
