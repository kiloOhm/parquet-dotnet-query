namespace Parquet.Query.Extensions.Writing.Attributes;

/// <summary>
/// Requests that an external indexing strategy build an index for the annotated column.
/// Use one of the built-in derived attributes where available, or this base attribute for custom strategies.
/// </summary>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = true, Inherited = true)]
public class ParquetExternalIndexAttribute : Attribute
{
    /// <summary>
    /// Initializes a new external index attribute.
    /// </summary>
    /// <param name="strategyName">The name of the external indexing strategy to invoke.</param>
    public ParquetExternalIndexAttribute(string strategyName)
    {
        if (string.IsNullOrWhiteSpace(strategyName))
        {
            throw new ArgumentException("Strategy name cannot be empty.", nameof(strategyName));
        }

        StrategyName = strategyName;
    }

    /// <summary>
    /// Gets the name of the external indexing strategy to invoke.
    /// </summary>
    public string StrategyName { get; }

    /// <summary>
    /// Gets or sets the relative order used when multiple external indexes target the same row type.
    /// </summary>
    public int Order { get; set; }
}
