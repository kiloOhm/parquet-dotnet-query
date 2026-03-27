using System.Linq.Expressions;
using Parquet.Query.Internal;

namespace Parquet.Query.Pushdown;

internal static class PushdownPredicateFactory
{
    public static ComparisonPushdownPredicate<T> CreateComparison<T>(
        LambdaExpression selector,
        ComparisonOperator @operator,
        object? value)
    {
        var path = ColumnPathResolver.FromLambda(selector);
        var rowPredicate = CompileComparison<T>(selector, @operator, value);
        var valueType = value?.GetType() ?? selector.Body.Type;

        return new ComparisonPushdownPredicate<T>(
            path.MemberPath,
            path.PhysicalPath,
            @operator,
            value,
            valueType,
            $"{path.MemberPath} {ToSymbol(@operator)} {FormatValue(value)}",
            rowPredicate);
    }

    public static StartsWithPushdownPredicate<T> CreateStartsWith<T>(
        Expression<Func<T, string?>> selector,
        string prefix)
    {
        ArgumentNullException.ThrowIfNull(prefix);

        var path = ColumnPathResolver.FromLambda(selector);
        var body = Expression.Call(
            selector.Body,
            nameof(string.StartsWith),
            Type.EmptyTypes,
            Expression.Constant(prefix),
            Expression.Constant(StringComparison.Ordinal));

        var rowPredicate = Expression.Lambda<Func<T, bool>>(body, selector.Parameters).Compile();

        return new StartsWithPushdownPredicate<T>(
            path.MemberPath,
            path.PhysicalPath,
            prefix,
            $"{path.MemberPath}.StartsWith({FormatValue(prefix)})",
            rowPredicate);
    }

    internal static object? ConvertValue(object? value, Type targetType)
    {
        if (value is null)
        {
            return null;
        }

        if (targetType.IsInstanceOfType(value))
        {
            return value;
        }

        if (targetType.IsEnum)
        {
            return value is string text
                ? Enum.Parse(targetType, text, ignoreCase: false)
                : Enum.ToObject(targetType, value);
        }

        return Convert.ChangeType(value, targetType);
    }

    internal static string FormatValue(object? value) =>
        value switch
        {
            null => "null",
            string text => $"\"{text}\"",
            DateTime dateTime => dateTime.ToString("O"),
            _ => value.ToString() ?? string.Empty
        };

    private static Func<T, bool> CompileComparison<T>(
        LambdaExpression selector,
        ComparisonOperator @operator,
        object? value)
    {
        var left = ColumnPathResolver.StripConvert(selector.Body);
        var right = CreateTypedConstant(left.Type, value);

        Expression body = @operator switch
        {
            ComparisonOperator.Equal => Expression.Equal(left, right),
            ComparisonOperator.NotEqual => Expression.NotEqual(left, right),
            ComparisonOperator.LessThan => Expression.LessThan(left, right),
            ComparisonOperator.LessThanOrEqual => Expression.LessThanOrEqual(left, right),
            ComparisonOperator.GreaterThan => Expression.GreaterThan(left, right),
            ComparisonOperator.GreaterThanOrEqual => Expression.GreaterThanOrEqual(left, right),
            _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
        };

        return Expression.Lambda<Func<T, bool>>(body, selector.Parameters).Compile();
    }

    private static Expression CreateTypedConstant(Type targetType, object? value)
    {
        if (value is null)
        {
            return Expression.Constant(null, targetType);
        }

        var nonNullableType = Nullable.GetUnderlyingType(targetType) ?? targetType;
        var convertedValue = ConvertValue(value, nonNullableType);
        var constant = Expression.Constant(convertedValue, nonNullableType);

        return nonNullableType == targetType
            ? constant
            : Expression.Convert(constant, targetType);
    }

    private static string ToSymbol(ComparisonOperator @operator) =>
        @operator switch
        {
            ComparisonOperator.Equal => "==",
            ComparisonOperator.NotEqual => "!=",
            ComparisonOperator.LessThan => "<",
            ComparisonOperator.LessThanOrEqual => "<=",
            ComparisonOperator.GreaterThan => ">",
            ComparisonOperator.GreaterThanOrEqual => ">=",
            _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
        };
}
