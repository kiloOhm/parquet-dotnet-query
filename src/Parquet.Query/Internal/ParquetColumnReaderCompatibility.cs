using Parquet.Schema;
using System.Reflection;

namespace Parquet.Query.Internal;

internal static class ParquetColumnReaderCompatibility
{
    public static Type GetLogicalType(DataField field)
    {
        var fieldType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;
        if (fieldType == typeof(ReadOnlyMemory<char>))
        {
            return typeof(string);
        }

        return fieldType == typeof(ReadOnlyMemory<byte>) ? typeof(byte[]) : fieldType;
    }

    public static object? GetLogicalValue(object? value) => value switch
    {
        ReadOnlyMemory<char> text => new string(text.ToArray()),
        ReadOnlyMemory<byte> bytes => bytes.ToArray(),
        _ => value
    };

    public static async Task<Array> ReadColumnAsync(
        QueryParquetRowGroupReader rowGroupReader,
        DataField field,
        CancellationToken cancellationToken)
    {
#if PARQUET_V6
        if (field.IsArray)
        {
            throw new NotSupportedException($"Direct reads of repeated column '{field.Path}' are not supported by parquet-dotnet v6.");
        }

        var rowCount = checked((int)rowGroupReader.RowCount);
        var fieldType = Nullable.GetUnderlyingType(field.ClrType) ?? field.ClrType;
        if (fieldType == typeof(string) || fieldType == typeof(ReadOnlyMemory<char>))
        {
            var values = new string?[rowCount];
            await rowGroupReader.ReadAsync(field, values.AsMemory(), cancellationToken: cancellationToken).ConfigureAwait(false);
            return values;
        }

        if (fieldType == typeof(byte[]) || fieldType == typeof(ReadOnlyMemory<byte>))
        {
            var values = new byte[]?[rowCount];
            await rowGroupReader.ReadAsync(field, values.AsMemory(), cancellationToken: cancellationToken).ConfigureAwait(false);
            return values;
        }

        if (!fieldType.IsValueType)
        {
            throw new NotSupportedException($"Direct reads of column type '{fieldType}' are not supported by parquet-dotnet v6.");
        }

        var method = typeof(ParquetColumnReaderCompatibility)
            .GetMethod(nameof(ReadValueColumnAsync), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(fieldType);
        return await (Task<Array>)method.Invoke(null, new object[] { rowGroupReader, field, cancellationToken })!;
#else
        var column = await rowGroupReader.ReadColumnAsync(field, cancellationToken).ConfigureAwait(false);
        return column.Data;
#endif
    }

#if PARQUET_V6
    private static async Task<Array> ReadValueColumnAsync<T>(
        ParquetRowGroupReader rowGroupReader,
        DataField field,
        CancellationToken cancellationToken)
        where T : struct
    {
        var rowCount = checked((int)rowGroupReader.RowCount);
        if (field.MaxDefinitionLevel > 0)
        {
            var values = new T?[rowCount];
            await rowGroupReader.ReadAsync(field, values.AsMemory(), cancellationToken: cancellationToken).ConfigureAwait(false);
            return values;
        }

        var requiredValues = new T[rowCount];
        await rowGroupReader.ReadAsync(field, requiredValues.AsMemory(), cancellationToken: cancellationToken).ConfigureAwait(false);
        return requiredValues;
    }
#endif
}
