using System.Linq.Expressions;

namespace Parquet.Query.Internal;

internal static class SourceAccessAnalyzer
{
    public static SourceAccessAnalysis Analyze(LambdaExpression expression)
    {
        Guard.NotNull(expression, nameof(expression));

        var visitor = new Visitor(expression.Parameters[0]);
        visitor.Visit(expression.Body);
        return new SourceAccessAnalysis(visitor.MemberPaths, visitor.RequiresFullMaterialization);
    }

    private sealed class Visitor : ExpressionVisitor
    {
        private readonly ParameterExpression _parameter;
        private readonly HashSet<string> _memberPaths = new(StringComparer.Ordinal);

        public Visitor(ParameterExpression parameter)
        {
            _parameter = parameter;
        }

        public IReadOnlyCollection<string> MemberPaths => _memberPaths;

        public bool RequiresFullMaterialization { get; private set; }

        public override Expression? Visit(Expression? node)
        {
            if (node is null || RequiresFullMaterialization)
            {
                return node;
            }

            return base.Visit(node);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (ColumnPathResolver.TryFromExpression(node, _parameter, out ColumnPath? path))
            {
                _memberPaths.Add(path!.MemberPath);
                return node;
            }

            return base.VisitMember(node);
        }

        protected override Expression VisitParameter(ParameterExpression node)
        {
            if (node == _parameter)
            {
                RequiresFullMaterialization = true;
            }

            return node;
        }
    }
}
