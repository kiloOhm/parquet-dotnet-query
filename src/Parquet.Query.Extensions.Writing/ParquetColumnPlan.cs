namespace Parquet.Query.Extensions.Writing;

/// <summary>
/// Describes how a source member maps to a parquet column.
/// </summary>
public sealed class ParquetColumnPlan
{
    /// <summary>
    /// Initializes a new column plan.
    /// </summary>
    /// <param name="memberPath">The CLR member path.</param>
    /// <param name="columnPath">The parquet column path.</param>
    /// <param name="memberType">The CLR type of the member.</param>
    /// <param name="attributes">The attributes discovered on the member.</param>
    public ParquetColumnPlan(
        string memberPath,
        string columnPath,
        Type memberType,
        IReadOnlyList<Attribute> attributes)
    {
        MemberPath = memberPath ?? throw new ArgumentNullException(nameof(memberPath));
        ColumnPath = columnPath ?? throw new ArgumentNullException(nameof(columnPath));
        MemberType = memberType ?? throw new ArgumentNullException(nameof(memberType));
        Attributes = attributes ?? throw new ArgumentNullException(nameof(attributes));
    }

    /// <summary>
    /// Gets the CLR member path.
    /// </summary>
    public string MemberPath { get; }

    /// <summary>
    /// Gets the parquet column path.
    /// </summary>
    public string ColumnPath { get; }

    /// <summary>
    /// Gets the CLR type of the member.
    /// </summary>
    public Type MemberType { get; }

    /// <summary>
    /// Gets the attributes discovered on the member.
    /// </summary>
    public IReadOnlyList<Attribute> Attributes { get; }
}
