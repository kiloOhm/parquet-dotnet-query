using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json.Serialization;

namespace Parquet.Query.Internal;

internal sealed record ColumnPath(string MemberPath, string PhysicalPath);

internal static class ColumnPathResolver
{
    public static ColumnPath FromLambda(LambdaExpression selector)
    {
        if (!TryFromExpression(selector.Body, selector.Parameters[0], out ColumnPath? path))
        {
            throw new NotSupportedException($"Only simple member access is supported in pushdown selectors. Received '{selector}'.");
        }

        return path!;
    }

    public static bool TryFromExpression(Expression expression, ParameterExpression parameter, out ColumnPath? path)
    {
        var members = new List<MemberInfo>();
        if (!TryCollectMembers(StripConvert(expression), parameter, members))
        {
            path = null;
            return false;
        }

        members.Reverse();

        path = new ColumnPath(
            string.Join(".", members.Select(member => member.Name)),
            string.Join("/", members.Select(GetColumnSegment)));

        return true;
    }

    internal static Expression StripConvert(Expression expression)
    {
        while (expression is UnaryExpression unaryExpression &&
            (unaryExpression.NodeType == ExpressionType.Convert ||
             unaryExpression.NodeType == ExpressionType.ConvertChecked))
        {
            expression = unaryExpression.Operand;
        }

        return expression;
    }

    private static bool TryCollectMembers(Expression expression, ParameterExpression parameter, List<MemberInfo> members)
    {
        if (expression == parameter)
        {
            return true;
        }

        if (expression is MemberExpression memberExpression && memberExpression.Expression is not null)
        {
            members.Add(memberExpression.Member);
            return TryCollectMembers(StripConvert(memberExpression.Expression), parameter, members);
        }

        return false;
    }

    private static string GetColumnSegment(MemberInfo member)
    {
        var nameAttribute = member.GetCustomAttribute<JsonPropertyNameAttribute>();
        return nameAttribute?.Name ?? member.Name;
    }
}
