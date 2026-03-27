using System.Collections.ObjectModel;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json.Serialization;
using Parquet.Query.Extensions.Writing.Attributes;
using Parquet.Schema;
using Parquet.Serialization;

namespace Parquet.Query.Extensions.Writing;

public static class ParquetWritePlanBuilder
{
    private static readonly ConcurrentDictionary<Type, CachedWritePlan> CachedPlans = new();
    private static readonly ConcurrentDictionary<Type, SerializableMember[]> SerializableMembers = new();

    public static ParquetSchema BuildSchema<T>() => BuildSchema(typeof(T));

    public static ParquetSchema BuildSchema(Type rowType)
    {
        ArgumentNullException.ThrowIfNull(rowType);
        return GetOrCreateCachedPlan(rowType).Schema;
    }

    public static ParquetWritePlan Build<T>(ParquetSerializerOptions? serializerOptions = null) => Build(typeof(T), serializerOptions);

    public static ParquetWritePlan Build(Type rowType, ParquetSerializerOptions? serializerOptions = null)
    {
        ArgumentNullException.ThrowIfNull(rowType);

        var cachedPlan = GetOrCreateCachedPlan(rowType);
        if (serializerOptions is null)
        {
            return cachedPlan.DefaultPlan;
        }

        return cachedPlan.CreatePlan(serializerOptions);
    }

    private static CachedWritePlan GetOrCreateCachedPlan(Type rowType) =>
        CachedPlans.GetOrAdd(rowType, static type => CachedWritePlan.Create(type, GetSerializableMembers));

    private static SerializableMember[] GetSerializableMembers(Type type) =>
        SerializableMembers.GetOrAdd(type, static currentType =>
            currentType.GetMembers(BindingFlags.Instance | BindingFlags.Public)
                .Where(member => member.MemberType is MemberTypes.Field or MemberTypes.Property)
                .Where(CanRead)
                .Where(member => member.GetCustomAttribute<JsonIgnoreAttribute>() is null)
                .Where(member => !member.GetCustomAttributes().Any(attribute => attribute.GetType().FullName == "Parquet.Serialization.Attributes.ParquetIgnoreAttribute"))
                .OrderBy(member => member.MetadataToken)
                .Select(member => new SerializableMember(
                    member,
                    GetMemberType(member),
                    member.GetCustomAttributes(inherit: true).OfType<Attribute>().ToArray(),
                    GetColumnName(member)))
                .ToArray());

    private static bool CanRead(MemberInfo member) => member switch
    {
        PropertyInfo property => property.GetMethod is not null,
        FieldInfo => true,
        _ => false
    };

    private static bool CanTraverse(Type type)
    {
        var unwrapped = UnwrapNullable(type);
        return !(unwrapped.IsPrimitive ||
            unwrapped.IsEnum ||
            unwrapped == typeof(string) ||
            unwrapped == typeof(decimal) ||
            unwrapped == typeof(DateTime) ||
            unwrapped == typeof(DateTimeOffset) ||
            unwrapped == typeof(TimeSpan) ||
            unwrapped == typeof(Guid) ||
            unwrapped == typeof(byte[]) ||
            typeof(System.Collections.IEnumerable).IsAssignableFrom(unwrapped));
    }

    private static Type GetMemberType(MemberInfo member) => member switch
    {
        PropertyInfo property => property.PropertyType,
        FieldInfo field => field.FieldType,
        _ => throw new NotSupportedException($"Member '{member.Name}' is not supported.")
    };

    private static string GetColumnName(MemberInfo member) =>
        member.GetCustomAttribute<JsonPropertyNameAttribute>()?.Name ?? member.Name;

    private static Type UnwrapNullable(Type type) => Nullable.GetUnderlyingType(type) ?? type;

    private sealed class CachedWritePlan
    {
        private CachedWritePlan(
            Type rowType,
            ParquetSchema schema,
            SerializerOptionsSnapshot defaultOptions,
            ReadOnlyCollection<ParquetColumnPlan> columns,
            ReadOnlyCollection<ParquetIndexDescriptor> indexDescriptors)
        {
            RowType = rowType;
            Schema = schema;
            DefaultOptions = defaultOptions;
            Columns = columns;
            IndexDescriptors = indexDescriptors;
            DefaultPlan = new ParquetWritePlan(rowType, schema, defaultOptions, columns, indexDescriptors);
        }

        public Type RowType { get; }

        public ParquetSchema Schema { get; }

        public SerializerOptionsSnapshot DefaultOptions { get; }

        public ReadOnlyCollection<ParquetColumnPlan> Columns { get; }

        public ReadOnlyCollection<ParquetIndexDescriptor> IndexDescriptors { get; }

        public ParquetWritePlan DefaultPlan { get; }

        public static CachedWritePlan Create(Type rowType, Func<Type, SerializableMember[]> getSerializableMembers)
        {
            var schema = Parquet.Serialization.TypeExtensions.GetParquetSchema(rowType, forWriting: false);
            var columns = new List<ParquetColumnPlan>();
            var indexes = new List<ParquetIndexDescriptor>();

            foreach (var column in EnumerateColumns(rowType, rowType, schema.Fields, prefix: null, getSerializableMembers))
            {
                columns.Add(new ParquetColumnPlan(
                    column.MemberPath,
                    column.ColumnPath,
                    column.MemberType,
                    Array.AsReadOnly(column.Attributes)));

                foreach (var descriptor in column.IndexDescriptors)
                {
                    indexes.Add(descriptor);
                }
            }

            var defaultOptions = CreateDefaultOptions(rowType);
            return new CachedWritePlan(
                rowType,
                schema,
                defaultOptions,
                new ReadOnlyCollection<ParquetColumnPlan>(columns),
                new ReadOnlyCollection<ParquetIndexDescriptor>(indexes));
        }

        public ParquetWritePlan CreatePlan(ParquetSerializerOptions serializerOptions) =>
            new(
                RowType,
                Schema,
                DefaultOptions.WithOverrides(serializerOptions),
                Columns,
                IndexDescriptors);

        private static SerializerOptionsSnapshot CreateDefaultOptions(Type rowType)
        {
            var options = new ParquetSerializerOptions();
            var attribute = rowType.GetCustomAttribute<ParquetWriteOptionsAttribute>();
            if (attribute is not null)
            {
                options.CompressionMethod = attribute.CompressionMethod;
                options.CompressionLevel = attribute.CompressionLevel;
                if (attribute.RowGroupSize > 0)
                {
                    options.RowGroupSize = attribute.RowGroupSize;
                }
            }

            return SerializerOptionsSnapshot.From(options);
        }

        private static IEnumerable<ResolvedColumn> EnumerateColumns(
            Type rootType,
            Type currentType,
            IReadOnlyList<Field> fields,
            string? prefix,
            Func<Type, SerializableMember[]> getSerializableMembers)
        {
            var members = getSerializableMembers(currentType);
            foreach (var member in members)
            {
                var memberPath = string.IsNullOrEmpty(prefix) ? member.Member.Name : $"{prefix}.{member.Member.Name}";
                var field = fields.FirstOrDefault(candidate => string.Equals(candidate.Name, member.ColumnName, StringComparison.Ordinal));

                if (field is not null)
                {
                    if (TryResolveDataField(field, out var dataField))
                    {
                        yield return new ResolvedColumn(
                            memberPath,
                            dataField!.Path.ToString(),
                            member.MemberType,
                            member.Attributes,
                            CreateIndexDescriptors(memberPath, dataField.Path.ToString(), member.Attributes));
                    }

                    if (field is StructField structField && CanTraverse(member.MemberType))
                    {
                        foreach (var nested in EnumerateColumns(rootType, UnwrapNullable(member.MemberType), structField.Fields, memberPath, getSerializableMembers))
                        {
                            yield return nested;
                        }
                    }
                }
            }
        }

        private static bool TryResolveDataField(Field field, out DataField? dataField)
        {
            dataField = field switch
            {
                DataField directDataField => directDataField,
                ListField listField when listField.Item is DataField itemDataField => itemDataField,
                _ => null
            };

            return dataField is not null;
        }

        private static ParquetIndexDescriptor[] CreateIndexDescriptors(
            string memberPath,
            string columnPath,
            IReadOnlyList<Attribute> attributes)
        {
            var descriptors = new List<ParquetIndexDescriptor>();

            if (attributes.OfType<ParquetBloomFilterAttribute>().Any())
            {
                descriptors.Add(new ParquetIndexDescriptor(
                    ParquetIndexKind.BloomFilter,
                    memberPath,
                    columnPath,
                    strategyName: null,
                    order: 0,
                    direction: null));
            }

            var sortKey = attributes.OfType<ParquetSortKeyAttribute>().SingleOrDefault();
            if (sortKey is not null)
            {
                descriptors.Add(new ParquetIndexDescriptor(
                    ParquetIndexKind.SortKey,
                    memberPath,
                    columnPath,
                    strategyName: null,
                    order: sortKey.Order,
                    direction: sortKey.Direction));
            }

            foreach (var externalIndex in attributes.OfType<ParquetExternalIndexAttribute>())
            {
                descriptors.Add(new ParquetIndexDescriptor(
                    ParquetIndexKind.External,
                    memberPath,
                    columnPath,
                    externalIndex.StrategyName,
                    externalIndex.Order,
                    direction: null));
            }

            return descriptors.ToArray();
        }
    }

    private sealed record SerializableMember(
        MemberInfo Member,
        Type MemberType,
        Attribute[] Attributes,
        string ColumnName);

    private sealed record ResolvedColumn(
        string MemberPath,
        string ColumnPath,
        Type MemberType,
        Attribute[] Attributes,
        ParquetIndexDescriptor[] IndexDescriptors);
}
