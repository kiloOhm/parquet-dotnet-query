namespace Parquet.Query.Extensions.Writing.Attributes;

[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true, Inherited = true)]
public sealed class ParquetExternalIndexAttribute : Attribute
{
    public ParquetExternalIndexAttribute(string strategyName)
    {
        if (string.IsNullOrWhiteSpace(strategyName))
        {
            throw new ArgumentException("Strategy name cannot be empty.", nameof(strategyName));
        }

        StrategyName = strategyName;
    }

    public string StrategyName { get; }

    public int Order { get; set; }
}
