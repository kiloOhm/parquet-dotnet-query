using System.Linq.Expressions;
using System.Reflection;
using Parquet.Query.Internal;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Expressions;

public static class PredicatePushdownExtractor
{
    public static PredicatePushdownSplit<T> Extract<T>(Expression<Func<T, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var pushdownPredicates = new List<PushdownPredicate<T>>();
        var residualExpressions = new List<Expression>();

        Collect(predicate.Body, predicate.Parameters[0], pushdownPredicates, residualExpressions);

        Expression<Func<T, bool>>? residualPredicate = null;
        if (residualExpressions.Count > 0)
        {
            var residualBody = residualExpressions.Aggregate(Expression.AndAlso);
            residualPredicate = Expression.Lambda<Func<T, bool>>(residualBody, predicate.Parameters);
        }

        return new PredicatePushdownSplit<T>(
            new PushdownFilter<T>(pushdownPredicates),
            residualPredicate,
            residualExpressions.Select(expression => expression.ToString()).ToArray());
    }

    private static void Collect<T>(
        Expression expression,
        ParameterExpression parameter,
        List<PushdownPredicate<T>> pushdownPredicates,
        List<Expression> residualExpressions)
    {
        expression = ColumnPathResolver.StripConvert(expression);

        if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso)
        {
            Collect(andAlso.Left, parameter, pushdownPredicates, residualExpressions);
            Collect(andAlso.Right, parameter, pushdownPredicates, residualExpressions);
            return;
        }

        if (TryTranslatePredicate<T>(expression, parameter, out PushdownPredicate<T>? predicate))
        {
            pushdownPredicates.Add(predicate!);
            return;
        }

        residualExpressions.Add(expression);
    }

    private static bool TryTranslatePredicate<T>(
        Expression expression,
        ParameterExpression parameter,
        out PushdownPredicate<T>? predicate)
    {
        if (expression is BinaryExpression binaryExpression &&
            TryMapOperator(binaryExpression.NodeType, out ComparisonOperator @operator) &&
            TryGetComparisonParts(binaryExpression.Left, binaryExpression.Right, parameter, @operator, out LambdaExpression? selector, out ComparisonOperator normalizedOperator, out object? value))
        {
            predicate = PushdownPredicateFactory.CreateComparison<T>(selector!, normalizedOperator, value);
            return true;
        }

        if (expression is MethodCallExpression methodCallExpression &&
            TryTranslateStartsWith<T>(methodCallExpression, parameter, out StartsWithPushdownPredicate<T>? startsWithPredicate))
        {
            predicate = startsWithPredicate;
            return true;
        }

        predicate = null;
        return false;
    }

    private static bool TryTranslateStartsWith<T>(
        MethodCallExpression expression,
        ParameterExpression parameter,
        out StartsWithPushdownPredicate<T>? predicate)
    {
        predicate = null;

        if (!string.Equals(expression.Method.Name, nameof(string.StartsWith), StringComparison.Ordinal))
        {
            return false;
        }

        if (expression.Object is null ||
            !ColumnPathResolver.TryFromExpression(expression.Object, parameter, out _) ||
            expression.Arguments.Count is < 1 or > 2 ||
            !TryEvaluateClosedValue(expression.Arguments[0], out object? prefixValue) ||
            prefixValue is not string prefix)
        {
            return false;
        }

        if (expression.Arguments.Count == 2)
        {
            if (!TryEvaluateClosedValue(expression.Arguments[1], out object? comparisonValue) ||
                comparisonValue is not StringComparison stringComparison ||
                stringComparison != StringComparison.Ordinal)
            {
                return false;
            }
        }

        predicate = PushdownPredicateFactory.CreateStartsWith(
            Expression.Lambda<Func<T, string?>>(expression.Object, parameter),
            prefix);

        return true;
    }

    private static bool TryGetComparisonParts(
        Expression left,
        Expression right,
        ParameterExpression parameter,
        ComparisonOperator @operator,
        out LambdaExpression? selector,
        out ComparisonOperator normalizedOperator,
        out object? value)
    {
        selector = null;
        normalizedOperator = @operator;
        value = null;

        if (ColumnPathResolver.TryFromExpression(left, parameter, out _) &&
            TryEvaluateClosedValue(right, out value))
        {
            selector = Expression.Lambda(left, parameter);
            return true;
        }

        if (ColumnPathResolver.TryFromExpression(right, parameter, out _) &&
            TryEvaluateClosedValue(left, out value))
        {
            selector = Expression.Lambda(right, parameter);
            normalizedOperator = Reverse(@operator);
            return true;
        }

        return false;
    }

    private static bool TryEvaluateClosedValue(Expression expression, out object? value)
    {
        expression = ColumnPathResolver.StripConvert(expression);

        if (expression is ConstantExpression constantExpression)
        {
            value = constantExpression.Value;
            return true;
        }

        if (expression is MemberExpression memberExpression)
        {
            if (memberExpression.Expression is null)
            {
                value = ReadMemberValue(null, memberExpression.Member);
                return true;
            }

            if (TryEvaluateClosedValue(memberExpression.Expression, out object? instance))
            {
                value = ReadMemberValue(instance, memberExpression.Member);
                return true;
            }
        }

        value = null;
        return false;
    }

    private static object? ReadMemberValue(object? instance, MemberInfo member) =>
        member switch
        {
            FieldInfo fieldInfo => fieldInfo.GetValue(instance),
            PropertyInfo propertyInfo => propertyInfo.GetValue(instance),
            _ => throw new NotSupportedException($"Unsupported member type '{member.MemberType}'.")
        };

    private static bool TryMapOperator(ExpressionType expressionType, out ComparisonOperator @operator)
    {
        @operator = expressionType switch
        {
            ExpressionType.Equal => ComparisonOperator.Equal,
            ExpressionType.NotEqual => ComparisonOperator.NotEqual,
            ExpressionType.LessThan => ComparisonOperator.LessThan,
            ExpressionType.LessThanOrEqual => ComparisonOperator.LessThanOrEqual,
            ExpressionType.GreaterThan => ComparisonOperator.GreaterThan,
            ExpressionType.GreaterThanOrEqual => ComparisonOperator.GreaterThanOrEqual,
            _ => default
        };

        return expressionType is ExpressionType.Equal or
            ExpressionType.NotEqual or
            ExpressionType.LessThan or
            ExpressionType.LessThanOrEqual or
            ExpressionType.GreaterThan or
            ExpressionType.GreaterThanOrEqual;
    }

    private static ComparisonOperator Reverse(ComparisonOperator @operator) =>
        @operator switch
        {
            ComparisonOperator.Equal => ComparisonOperator.Equal,
            ComparisonOperator.NotEqual => ComparisonOperator.NotEqual,
            ComparisonOperator.LessThan => ComparisonOperator.GreaterThan,
            ComparisonOperator.LessThanOrEqual => ComparisonOperator.GreaterThanOrEqual,
            ComparisonOperator.GreaterThan => ComparisonOperator.LessThan,
            ComparisonOperator.GreaterThanOrEqual => ComparisonOperator.LessThanOrEqual,
            _ => throw new ArgumentOutOfRangeException(nameof(@operator), @operator, null)
        };
}
