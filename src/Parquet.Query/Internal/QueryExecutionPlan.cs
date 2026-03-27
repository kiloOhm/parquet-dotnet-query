using System.Linq.Expressions;
using Parquet;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;
using Parquet.Serialization;

namespace Parquet.Query.Internal;

internal sealed class QueryExecutionPlan<TSource>
    where TSource : class, new()
{
    public QueryExecutionPlan(
        ParquetQueryPlan queryPlan,
        IReadOnlyList<QueryExecutionFilePlan<TSource>> files)
    {
        QueryPlan = queryPlan;
        Files = files;
    }

    public ParquetQueryPlan QueryPlan { get; }

    public IReadOnlyList<QueryExecutionFilePlan<TSource>> Files { get; }
}

internal sealed class QueryExecutionFilePlan<TSource>
    where TSource : class, new()
{
    public QueryExecutionFilePlan(
        string filePath,
        QueryFilePlan filePlan,
        SourceMaterializationPlan<TSource>? materializationPlan)
    {
        FilePath = filePath;
        FilePlan = filePlan;
        MaterializationPlan = materializationPlan;
    }

    public string FilePath { get; }

    public QueryFilePlan FilePlan { get; }

    public SourceMaterializationPlan<TSource>? MaterializationPlan { get; }
}

internal sealed class OpenQueryExecutionFile<TSource>
    where TSource : class, new()
{
    public OpenQueryExecutionFile(
        QueryExecutionFilePlan<TSource> executionFilePlan,
        ParquetReader reader,
        ParquetSerializerOptions? serializerOptions)
    {
        ExecutionFilePlan = executionFilePlan;
        Reader = reader;
        SerializerOptions = serializerOptions;
    }

    public QueryExecutionFilePlan<TSource> ExecutionFilePlan { get; }

    public ParquetReader Reader { get; }

    public ParquetSerializerOptions? SerializerOptions { get; }
}

internal sealed class ProjectionPlan<TSource, TResult>
    where TSource : class, new()
{
    private readonly VectorizedProjectionNode<TResult>? _vectorizedProjector;
    private readonly Func<TSource, TResult>? _rowProjector;
    private readonly Func<object?, TResult>? _valueProjector;

    private ProjectionPlan(
        VectorizedProjectionNode<TResult>? vectorizedProjector,
        Func<TSource, TResult>? rowProjector,
        Func<object?, TResult>? valueProjector,
        SourceColumnBinding<TSource>? directScalarBinding,
        IReadOnlyList<SourceColumnBinding<TSource>> vectorizedBindings)
    {
        _vectorizedProjector = vectorizedProjector;
        _rowProjector = rowProjector;
        _valueProjector = valueProjector;
        DirectScalarBinding = directScalarBinding;
        VectorizedBindings = vectorizedBindings;
    }

    public SourceColumnBinding<TSource>? DirectScalarBinding { get; }

    public bool IsDirectScalar => DirectScalarBinding is not null;

    public bool IsVectorized => _vectorizedProjector is not null;

    public IReadOnlyList<SourceColumnBinding<TSource>> VectorizedBindings { get; }

    public static ProjectionPlan<TSource, TResult> Create(
        Expression<Func<TSource, TResult>>? projection,
        SourceMaterializationPlan<TSource> materializationPlan)
    {
        if (projection is null)
        {
            return new ProjectionPlan<TSource, TResult>(
                vectorizedProjector: null,
                static row => (TResult)(object)row,
                value => value is null ? default! : (TResult)value,
                directScalarBinding: null,
                vectorizedBindings: Array.Empty<SourceColumnBinding<TSource>>());
        }

        if (!materializationPlan.RequiresFullMaterialization &&
            TryCreateDirectScalarProjection(projection, materializationPlan, out SourceColumnBinding<TSource>? binding, out Func<object?, TResult>? valueProjector))
        {
            return new ProjectionPlan<TSource, TResult>(
                vectorizedProjector: null,
                projection.Compile(),
                valueProjector,
                binding,
                vectorizedBindings: Array.Empty<SourceColumnBinding<TSource>>());
        }

        if (!materializationPlan.RequiresFullMaterialization &&
            TryCreateVectorizedProjection(projection, materializationPlan, out VectorizedProjectionNode<TResult>? vectorizedProjector, out IReadOnlyList<SourceColumnBinding<TSource>> vectorizedBindings))
        {
            return new ProjectionPlan<TSource, TResult>(
                vectorizedProjector,
                projection.Compile(),
                valueProjector: null,
                directScalarBinding: null,
                vectorizedBindings);
        }

        return new ProjectionPlan<TSource, TResult>(
            vectorizedProjector: null,
            projection.Compile(),
            valueProjector: null,
            directScalarBinding: null,
            vectorizedBindings: Array.Empty<SourceColumnBinding<TSource>>());
    }

    public TResult ProjectRow(TSource row) =>
        _rowProjector is not null
            ? _rowProjector(row)
            : throw new InvalidOperationException("A row projector was not available for this projection plan.");

    public TResult ProjectValue(object? value) =>
        _valueProjector is not null
            ? _valueProjector(value)
            : throw new InvalidOperationException("A direct value projector was not available for this projection plan.");

    public TResult ProjectVectorized(IReadOnlyDictionary<string, object?> valuesByMemberPath) =>
        _vectorizedProjector is not null
            ? _vectorizedProjector.Evaluate(valuesByMemberPath)
            : throw new InvalidOperationException("A vectorized projector was not available for this projection plan.");

    private static bool TryCreateDirectScalarProjection(
        Expression<Func<TSource, TResult>> projection,
        SourceMaterializationPlan<TSource> materializationPlan,
        out SourceColumnBinding<TSource>? binding,
        out Func<object?, TResult>? valueProjector)
    {
        binding = null;
        valueProjector = null;

        var body = ColumnPathResolver.StripConvert(projection.Body);
        if (!ColumnPathResolver.TryFromExpression(body, projection.Parameters[0], out ColumnPath? path))
        {
            return false;
        }

        binding = materializationPlan.FindBinding(path!.MemberPath);
        if (binding is null)
        {
            return false;
        }

        valueProjector = value =>
        {
            if (value is null)
            {
                return default!;
            }

            var targetType = Nullable.GetUnderlyingType(typeof(TResult)) ?? typeof(TResult);
            if (targetType.IsInstanceOfType(value))
            {
                return (TResult)value;
            }

            var convertedValue = PushdownPredicateFactory.ConvertValue(value, targetType);
            return (TResult)convertedValue!;
        };
        return true;
    }

    private static bool TryCreateVectorizedProjection(
        Expression<Func<TSource, TResult>> projection,
        SourceMaterializationPlan<TSource> materializationPlan,
        out VectorizedProjectionNode<TResult>? vectorizedProjector,
        out IReadOnlyList<SourceColumnBinding<TSource>> bindings)
    {
        var uniqueBindings = new Dictionary<string, SourceColumnBinding<TSource>>(StringComparer.Ordinal);
        if (!TryCreateNode(projection.Body, projection.Parameters[0], materializationPlan, uniqueBindings, out var node))
        {
            vectorizedProjector = null;
            bindings = Array.Empty<SourceColumnBinding<TSource>>();
            return false;
        }

        vectorizedProjector = WrapNode(node!);
        bindings = uniqueBindings.Values.OrderBy(binding => binding.MemberPath, StringComparer.Ordinal).ToArray();
        return true;
    }

    private static bool TryCreateNode(
        Expression expression,
        ParameterExpression parameter,
        SourceMaterializationPlan<TSource> materializationPlan,
        IDictionary<string, SourceColumnBinding<TSource>> bindings,
        out VectorizedProjectionNode? node)
    {
        expression = ColumnPathResolver.StripConvert(expression);

        if (TryCreateBindingNode(expression, parameter, materializationPlan, bindings, out node))
        {
            return true;
        }

        if (expression is ConstantExpression constantExpression)
        {
            node = VectorizedProjectionNode.Constant(constantExpression.Value);
            return true;
        }

        if (expression is NewExpression newExpression &&
            newExpression.Constructor is not null &&
            TryCreateConstructorNode(newExpression, parameter, materializationPlan, bindings, out node))
        {
            return true;
        }

        if (expression is MemberInitExpression memberInitExpression &&
            TryCreateMemberInitNode(memberInitExpression, parameter, materializationPlan, bindings, out node))
        {
            return true;
        }

        node = null;
        return false;
    }

    private static bool TryCreateBindingNode(
        Expression expression,
        ParameterExpression parameter,
        SourceMaterializationPlan<TSource> materializationPlan,
        IDictionary<string, SourceColumnBinding<TSource>> bindings,
        out VectorizedProjectionNode? node)
    {
        node = null;

        if (!ColumnPathResolver.TryFromExpression(expression, parameter, out ColumnPath? path))
        {
            return false;
        }

        var binding = materializationPlan.FindBinding(path!.MemberPath);
        if (binding is null || binding.RequiresFullRowRead)
        {
            return false;
        }

        bindings[binding.MemberPath] = binding;
        node = VectorizedProjectionNode.Binding(
            binding.MemberPath,
            Nullable.GetUnderlyingType(expression.Type) ?? expression.Type);
        return true;
    }

    private static bool TryCreateConstructorNode(
        NewExpression newExpression,
        ParameterExpression parameter,
        SourceMaterializationPlan<TSource> materializationPlan,
        IDictionary<string, SourceColumnBinding<TSource>> bindings,
        out VectorizedProjectionNode? node)
    {
        var argumentNodes = new VectorizedProjectionNode[newExpression.Arguments.Count];
        for (var index = 0; index < newExpression.Arguments.Count; index++)
        {
            if (!TryCreateNode(newExpression.Arguments[index], parameter, materializationPlan, bindings, out var argumentNode))
            {
                node = null;
                return false;
            }

            argumentNodes[index] = argumentNode!;
        }

        node = VectorizedProjectionNode.Constructor(newExpression.Constructor!, argumentNodes);
        return true;
    }

    private static bool TryCreateMemberInitNode(
        MemberInitExpression memberInitExpression,
        ParameterExpression parameter,
        SourceMaterializationPlan<TSource> materializationPlan,
        IDictionary<string, SourceColumnBinding<TSource>> bindings,
        out VectorizedProjectionNode? node)
    {
        if (memberInitExpression.NewExpression.Constructor is null ||
            memberInitExpression.NewExpression.Arguments.Count != 0 ||
            memberInitExpression.Bindings.Any(binding => binding.BindingType != MemberBindingType.Assignment))
        {
            node = null;
            return false;
        }

        var assignments = new List<VectorizedMemberAssignment>();
        foreach (var binding in memberInitExpression.Bindings.Cast<MemberAssignment>())
        {
            if (!TryCreateNode(binding.Expression, parameter, materializationPlan, bindings, out var childNode))
            {
                node = null;
                return false;
            }

            assignments.Add(new VectorizedMemberAssignment(binding.Member, childNode!));
        }

        node = VectorizedProjectionNode.MemberInit(memberInitExpression.NewExpression.Constructor, assignments);
        return true;
    }

    private static VectorizedProjectionNode<TResult> WrapNode(VectorizedProjectionNode node) =>
        new(values =>
        {
            var value = node.EvaluateUntyped(values);
            if (value is null)
            {
                return default!;
            }

            if (value is TResult typedValue)
            {
                return typedValue;
            }

            var targetType = Nullable.GetUnderlyingType(typeof(TResult)) ?? typeof(TResult);
            return (TResult)PushdownPredicateFactory.ConvertValue(value, targetType)!;
        });
}

internal sealed class VectorizedProjectionNode<TResult>
{
    private readonly Func<IReadOnlyDictionary<string, object?>, TResult> _evaluator;

    public VectorizedProjectionNode(Func<IReadOnlyDictionary<string, object?>, TResult> evaluator)
    {
        _evaluator = evaluator;
    }

    public TResult Evaluate(IReadOnlyDictionary<string, object?> values) => _evaluator(values);
}

internal sealed class VectorizedProjectionNode
{
    private readonly Func<IReadOnlyDictionary<string, object?>, object?> _evaluator;

    private VectorizedProjectionNode(Func<IReadOnlyDictionary<string, object?>, object?> evaluator)
    {
        _evaluator = evaluator;
    }

    public object? EvaluateUntyped(IReadOnlyDictionary<string, object?> values) => _evaluator(values);

    public static VectorizedProjectionNode Constant(object? value) => new(_ => value);

    public static VectorizedProjectionNode Binding(string memberPath, Type targetType) =>
        new(values =>
        {
            values.TryGetValue(memberPath, out var value);
            if (value is null)
            {
                return null;
            }

            if (targetType.IsInstanceOfType(value))
            {
                return value;
            }

            return PushdownPredicateFactory.ConvertValue(value, targetType);
        });

    public static VectorizedProjectionNode Constructor(
        System.Reflection.ConstructorInfo constructor,
        IReadOnlyList<VectorizedProjectionNode> arguments) =>
        new(values =>
        {
            var argumentValues = new object?[arguments.Count];
            for (var index = 0; index < arguments.Count; index++)
            {
                argumentValues[index] = arguments[index].EvaluateUntyped(values);
            }

            return constructor.Invoke(argumentValues);
        });

    public static VectorizedProjectionNode MemberInit(
        System.Reflection.ConstructorInfo constructor,
        IReadOnlyList<VectorizedMemberAssignment> assignments) =>
        new(values =>
        {
            var instance = constructor.Invoke(Array.Empty<object?>());
            foreach (var assignment in assignments)
            {
                var value = assignment.Value.EvaluateUntyped(values);
                assignment.Assign(instance, value);
            }

            return instance;
        });
}

internal sealed class VectorizedMemberAssignment
{
    private readonly Action<object, object?> _assign;

    public VectorizedMemberAssignment(System.Reflection.MemberInfo member, VectorizedProjectionNode value)
    {
        Value = value;
        _assign = CreateAssigner(member);
    }

    public VectorizedProjectionNode Value { get; }

    public void Assign(object instance, object? value) => _assign(instance, value);

    private static Action<object, object?> CreateAssigner(System.Reflection.MemberInfo member) =>
        member switch
        {
            System.Reflection.PropertyInfo property when property.SetMethod is not null => (instance, value) =>
                property.SetValue(instance, ConvertValue(value, property.PropertyType)),
            System.Reflection.FieldInfo field => (instance, value) =>
                field.SetValue(instance, ConvertValue(value, field.FieldType)),
            _ => throw new NotSupportedException($"Vectorized projection cannot assign member '{member.Name}'.")
        };

    private static object? ConvertValue(object? value, Type destinationType)
    {
        if (value is null)
        {
            return null;
        }

        var targetType = Nullable.GetUnderlyingType(destinationType) ?? destinationType;
        if (targetType.IsInstanceOfType(value))
        {
            return value;
        }

        return PushdownPredicateFactory.ConvertValue(value, targetType);
    }
}
