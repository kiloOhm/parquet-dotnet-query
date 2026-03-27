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
        var residualDiagnostics = new List<PredicatePushdownDiagnostic>();

        Collect(predicate.Body, predicate.Parameters[0], pushdownPredicates, residualDiagnostics);

        Expression<Func<T, bool>>? residualPredicate = null;
        if (residualDiagnostics.Count > 0)
        {
            var residualBody = residualDiagnostics
                .Select(diagnostic => diagnostic.Expression)
                .Aggregate(Expression.AndAlso);
            residualPredicate = Expression.Lambda<Func<T, bool>>(residualBody, predicate.Parameters);
        }

        return new PredicatePushdownSplit<T>(
            new PushdownFilter<T>(pushdownPredicates),
            residualPredicate,
            residualDiagnostics);
    }

    private static void Collect<T>(
        Expression expression,
        ParameterExpression parameter,
        List<PushdownPredicate<T>> pushdownPredicates,
        List<PredicatePushdownDiagnostic> residualDiagnostics)
    {
        expression = ColumnPathResolver.StripConvert(expression);

        if (expression is BinaryExpression { NodeType: ExpressionType.AndAlso } andAlso)
        {
            Collect(andAlso.Left, parameter, pushdownPredicates, residualDiagnostics);
            Collect(andAlso.Right, parameter, pushdownPredicates, residualDiagnostics);
            return;
        }

        if (TryTranslatePredicate<T>(expression, parameter, out PushdownPredicate<T>? predicate))
        {
            pushdownPredicates.Add(predicate!);
            return;
        }

        residualDiagnostics.Add(new PredicatePushdownDiagnostic(expression, ExplainUnsupported(expression, parameter)));
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

    private static string ExplainUnsupported(Expression expression, ParameterExpression parameter)
    {
        expression = ColumnPathResolver.StripConvert(expression);

        if (expression is BinaryExpression { NodeType: ExpressionType.OrElse })
        {
            return "Logical OR predicates are not pushed down yet; only AND chains are extractable.";
        }

        if (expression is UnaryExpression { NodeType: ExpressionType.Not })
        {
            return "Negated predicates are not normalized for pushdown yet.";
        }

        if (expression is MethodCallExpression methodCallExpression)
        {
            if (string.Equals(methodCallExpression.Method.Name, nameof(string.StartsWith), StringComparison.Ordinal))
            {
                if (methodCallExpression.Arguments.Count == 2 &&
                    TryEvaluateClosedValue(methodCallExpression.Arguments[1], out object? comparisonValue) &&
                    comparisonValue is not StringComparison.Ordinal)
                {
                    return "string.StartsWith is only pushdown-eligible with StringComparison.Ordinal.";
                }

                return "string.StartsWith pushdown requires a direct member access and a closed-over string prefix.";
            }

            return "Method calls are not pushdown-eligible except string.StartsWith(..., StringComparison.Ordinal).";
        }

        if (expression is BinaryExpression binaryExpression)
        {
            if (!TryMapOperator(binaryExpression.NodeType, out ComparisonOperator @operator))
            {
                return "Only ==, !=, <, <=, >, and >= comparisons are pushdown-eligible.";
            }

            if ((ColumnPathResolver.TryFromExpression(binaryExpression.Left, parameter, out _) &&
                 !TryEvaluateClosedValue(binaryExpression.Right, out _)) ||
                (ColumnPathResolver.TryFromExpression(binaryExpression.Right, parameter, out _) &&
                 !TryEvaluateClosedValue(binaryExpression.Left, out _)))
            {
                return "Pushdown comparisons must compare a direct member access to a closed-over constant value.";
            }

            if ((TryEvaluateClosedValue(binaryExpression.Left, out _) &&
                 !ColumnPathResolver.TryFromExpression(binaryExpression.Right, parameter, out _)) ||
                (TryEvaluateClosedValue(binaryExpression.Right, out _) &&
                 !ColumnPathResolver.TryFromExpression(binaryExpression.Left, parameter, out _)))
            {
                return "Pushdown comparisons must reference a direct source member on one side of the comparison.";
            }

            _ = @operator;
            return "This comparison shape is not pushdown-eligible.";
        }

        if (expression == parameter)
        {
            return "Whole-row predicates require full materialization and cannot be pushed down.";
        }

        return "This predicate shape is not pushdown-eligible.";
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
