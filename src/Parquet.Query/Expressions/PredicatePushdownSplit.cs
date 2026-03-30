using System.Linq.Expressions;
using System.Linq;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Expressions;

/// <summary>
/// Captures the pushdownable and residual parts of a predicate expression.
/// </summary>
/// <typeparam name="T">The source row type the predicate targets.</typeparam>
public sealed class PredicatePushdownSplit<T>
{
    /// <summary>
    /// Initializes a new split result.
    /// </summary>
    /// <param name="pushdownFilter">The predicates that can be planned as pushdown operations.</param>
    /// <param name="residualPredicate">The residual predicate that still requires row evaluation.</param>
    /// <param name="diagnostics">Diagnostics for expressions that could not be pushed down.</param>
    public PredicatePushdownSplit(
        PushdownFilter<T> pushdownFilter,
        Expression<Func<T, bool>>? residualPredicate,
        IReadOnlyList<PredicatePushdownDiagnostic> diagnostics)
    {
        PushdownFilter = pushdownFilter;
        ResidualPredicate = residualPredicate;
        Diagnostics = diagnostics;
        UnsupportedExpressions = diagnostics.Select(diagnostic => diagnostic.ExpressionText).ToArray();
    }

    /// <summary>
    /// Gets the predicates that can be pushed down.
    /// </summary>
    public PushdownFilter<T> PushdownFilter { get; }

    /// <summary>
    /// Gets the residual predicate that must be evaluated after reading rows.
    /// </summary>
    public Expression<Func<T, bool>>? ResidualPredicate { get; }

    /// <summary>
    /// Gets diagnostics describing why expressions could not be pushed down.
    /// </summary>
    public IReadOnlyList<PredicatePushdownDiagnostic> Diagnostics { get; }

    /// <summary>
    /// Gets the textual representation of unsupported expressions.
    /// </summary>
    public IReadOnlyList<string> UnsupportedExpressions { get; }
}

/// <summary>
/// Describes a single expression that could not be translated into pushdown.
/// </summary>
public sealed class PredicatePushdownDiagnostic
{
    /// <summary>
    /// Initializes a new diagnostic for an unsupported expression.
    /// </summary>
    /// <param name="expression">The unsupported expression.</param>
    /// <param name="reason">A human-readable explanation.</param>
    public PredicatePushdownDiagnostic(Expression expression, string reason)
    {
        Expression = expression;
        ExpressionText = expression.ToString();
        Reason = reason;
    }

    /// <summary>
    /// Gets the original unsupported expression.
    /// </summary>
    public Expression Expression { get; }

    /// <summary>
    /// Gets the string form of <see cref="Expression"/>.
    /// </summary>
    public string ExpressionText { get; }

    /// <summary>
    /// Gets the explanation for why the expression could not be pushed down.
    /// </summary>
    public string Reason { get; }

    /// <summary>
    /// Returns the expression text and diagnostic reason.
    /// </summary>
    /// <returns>A human-readable diagnostic message.</returns>
    public override string ToString() => $"{ExpressionText} => {Reason}";
}
