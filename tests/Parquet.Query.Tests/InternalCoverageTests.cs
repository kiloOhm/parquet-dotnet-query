using System.Reflection;
using Parquet.Query;
using Parquet.Serialization;

namespace Parquet.Query.Tests;

public sealed class InternalCoverageTests
{
    [Fact]
    public void PagePruner_decode_primitive_handles_multiple_supported_types()
    {
        Assert.Equal((byte)7, InvokeDecodePrimitive(typeof(byte), new byte[] { 7 }));
        Assert.Equal((sbyte)-2, InvokeDecodePrimitive(typeof(sbyte), new byte[] { 0xFE }));
        Assert.Equal((short)42, InvokeDecodePrimitive(typeof(short), BitConverter.GetBytes((short)42)));
        Assert.Equal((ushort)42, InvokeDecodePrimitive(typeof(ushort), BitConverter.GetBytes((ushort)42)));
        Assert.Equal(42, InvokeDecodePrimitive(typeof(int), BitConverter.GetBytes(42)));
        Assert.Equal((uint)42, InvokeDecodePrimitive(typeof(uint), BitConverter.GetBytes((uint)42)));
        Assert.Equal(42L, InvokeDecodePrimitive(typeof(long), BitConverter.GetBytes(42L)));
        Assert.Equal((ulong)42, InvokeDecodePrimitive(typeof(ulong), BitConverter.GetBytes((ulong)42)));
        Assert.Equal(1.5f, InvokeDecodePrimitive(typeof(float), BitConverter.GetBytes(1.5f)));
        Assert.Equal(2.5d, InvokeDecodePrimitive(typeof(double), BitConverter.GetBytes(2.5d)));
        Assert.Equal(true, InvokeDecodePrimitive(typeof(bool), new byte[] { 1 }));
        Assert.Equal("hi", InvokeDecodePrimitive(typeof(string), System.Text.Encoding.UTF8.GetBytes("hi")));
        Assert.Null(InvokeDecodePrimitive(typeof(decimal), new byte[] { 1, 2, 3, 4 }));
        Assert.Null(InvokeDecodePrimitive(typeof(int), new byte[] { 1 }));
    }

    [Fact]
    public void PagePruner_helpers_cover_edge_cases()
    {
        var upperBound = InvokeGetOrdinalUpperBound("ab");
        Assert.Equal("ac", upperBound);
        Assert.Null(InvokeGetOrdinalUpperBound(string.Empty));
        Assert.Null(InvokeGetOrdinalUpperBound(new string(char.MaxValue, 2)));
    }

    [Fact]
    public async Task PlanAsync_falls_back_to_full_materialization_for_read_only_properties()
    {
        var filePath = Path.GetTempFileName();
        System.IO.File.Delete(filePath);
        filePath = Path.ChangeExtension(filePath, ".parquet");

        try
        {
            await ParquetSerializer.SerializeAsync(
                new[] { new TestRow { Id = 1, Name = "alpha", Country = "DE", Age = 10 } },
                filePath);

            var plan = await ParquetQuery
                .FromFile<ReadOnlyPropertyRow>(filePath)
                .PlanAsync();

            Assert.True(plan.RequiresFullMaterialization);
        }
        finally
        {
            if (System.IO.File.Exists(filePath))
            {
                System.IO.File.Delete(filePath);
            }
        }
    }

    [Fact]
    public async Task PlanAsync_falls_back_to_full_materialization_for_non_scalar_predicates()
    {
        var filePath = Path.GetTempFileName();
        System.IO.File.Delete(filePath);
        filePath = Path.ChangeExtension(filePath, ".parquet");

        try
        {
            await ParquetSerializer.SerializeAsync(
                new[]
                {
                    new ComplexPredicateRow { Id = 1, Address = new TestAddress { City = "Berlin", PostalCode = "10115" } }
                },
                filePath);

            var plan = await ParquetQuery
                .FromFile<ComplexPredicateRow>(filePath)
                .Where(row => row.Address == null)
                .Select(row => row.Id)
                .PlanAsync();

            Assert.True(plan.RequiresFullMaterialization);
        }
        finally
        {
            if (System.IO.File.Exists(filePath))
            {
                System.IO.File.Delete(filePath);
            }
        }
    }

    [Fact]
    public async Task PlanAsync_falls_back_to_full_materialization_for_nested_types_without_default_constructor()
    {
        var filePath = Path.GetTempFileName();
        System.IO.File.Delete(filePath);
        filePath = Path.ChangeExtension(filePath, ".parquet");

        try
        {
            await ParquetSerializer.SerializeAsync(
                new[]
                {
                    new NoDefaultCtorNestedRow { Id = 1, Address = new NoDefaultCtorAddress("Berlin") }
                },
                filePath);

            var plan = await ParquetQuery
                .FromFile<NoDefaultCtorNestedRow>(filePath)
                .PlanAsync();

            Assert.True(plan.RequiresFullMaterialization);
        }
        finally
        {
            if (System.IO.File.Exists(filePath))
            {
                System.IO.File.Delete(filePath);
            }
        }
    }

    private static object? InvokeDecodePrimitive(Type type, byte[] bytes)
    {
        var pagePrunerType = typeof(ParquetQuery).Assembly.GetType("Parquet.Query.Planning.PagePruner", throwOnError: true)
            ?? throw new InvalidOperationException("PagePruner type not found.");
        var method = pagePrunerType.GetMethod("DecodePrimitive", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException("DecodePrimitive not found.");
        return method.Invoke(null, new object[] { type, bytes });
    }

    private static string? InvokeGetOrdinalUpperBound(string prefix)
    {
        var pagePrunerType = typeof(ParquetQuery).Assembly.GetType("Parquet.Query.Planning.PagePruner", throwOnError: true)
            ?? throw new InvalidOperationException("PagePruner type not found.");
        var method = pagePrunerType.GetMethod("GetOrdinalUpperBound", BindingFlags.NonPublic | BindingFlags.Static)
            ?? throw new InvalidOperationException("GetOrdinalUpperBound not found.");
        return (string?)method.Invoke(null, new object[] { prefix });
    }
}
