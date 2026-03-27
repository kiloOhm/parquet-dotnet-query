using System.Linq.Expressions;
using System.Linq;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Expressions;

public sealed class PredicatePushdownSplit<T>
{
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

    public PushdownFilter<T> PushdownFilter { get; }

    public Expression<Func<T, bool>>? ResidualPredicate { get; }

    public IReadOnlyList<PredicatePushdownDiagnostic> Diagnostics { get; }

    public IReadOnlyList<string> UnsupportedExpressions { get; }
}

public sealed class PredicatePushdownDiagnostic
{
    public PredicatePushdownDiagnostic(Expression expression, string reason)
    {
        Expression = expression;
        ExpressionText = expression.ToString();
        Reason = reason;
    }

    public Expression Expression { get; }

    public string ExpressionText { get; }

    public string Reason { get; }

    public override string ToString() => $"{ExpressionText} => {Reason}";
}
