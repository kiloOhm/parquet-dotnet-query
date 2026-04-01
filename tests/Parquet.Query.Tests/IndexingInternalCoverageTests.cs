using System.Reflection;
using Parquet.Query.Extensions.Indexing;

namespace Parquet.Query.Tests;

public sealed class IndexingInternalCoverageTests
{
    [Fact]
    public void FooterIndexValueFormatter_reports_supported_types()
    {
        Assert.True(InvokeIsSupportedType(typeof(string)));
        Assert.True(InvokeIsSupportedType(typeof(int?)));
        Assert.True(InvokeIsSupportedType(typeof(Guid)));
        Assert.True(InvokeIsSupportedType(typeof(byte[])));
        Assert.True(InvokeIsSupportedType(typeof(ConsoleColor)));
        Assert.False(InvokeIsSupportedType(typeof(TestAddress)));
    }

    [Fact]
    public void FooterIndexValueFormatter_formats_supported_values()
    {
        var timestamp = new DateTime(2024, 01, 02, 03, 04, 05, DateTimeKind.Local);
        var offsetTimestamp = new DateTimeOffset(2024, 01, 02, 03, 04, 05, TimeSpan.FromHours(2));

        Assert.Equal((true, "hello"), InvokeTryFormat("hello"));
        Assert.Equal((true, "AQI="), InvokeTryFormat(new byte[] { 1, 2 }));
        Assert.Equal((true, ConsoleColor.DarkGreen.ToString()), InvokeTryFormat(ConsoleColor.DarkGreen));
        Assert.Equal((true, timestamp.ToUniversalTime().ToString("O")), InvokeTryFormat(timestamp));
        Assert.Equal((true, offsetTimestamp.ToUniversalTime().ToString("O")), InvokeTryFormat(offsetTimestamp));
        Assert.Equal((true, TimeSpan.FromMinutes(90).ToString("c")), InvokeTryFormat(TimeSpan.FromMinutes(90)));
        Assert.Equal((true, "42"), InvokeTryFormat(42));
        Assert.Equal((false, string.Empty), InvokeTryFormat(new object()));
    }

    private static Type FormatterType =>
        typeof(FooterIndexQueryExtensions).Assembly.GetType("Parquet.Query.Extensions.Indexing.FooterIndexValueFormatter", throwOnError: true)
        ?? throw new InvalidOperationException("FooterIndexValueFormatter type not found.");

    private static bool InvokeIsSupportedType(Type type)
    {
        var method = FormatterType.GetMethod("IsSupportedType", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("IsSupportedType not found.");
        return (bool)method.Invoke(null, new object[] { type })!;
    }

    private static (bool Success, string Formatted) InvokeTryFormat(object? value)
    {
        var method = FormatterType.GetMethod("TryFormat", BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic)
            ?? throw new InvalidOperationException("TryFormat not found.");
        var arguments = new object?[] { value, null };
        var success = (bool)method.Invoke(null, arguments)!;
        return (success, (string)(arguments[1] ?? string.Empty));
    }

}
