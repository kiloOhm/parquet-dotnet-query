namespace Parquet.Query.Tests;

public sealed class NestedTestRow
{
    public int Id { get; set; }

    public string Country { get; set; } = string.Empty;

    public TestAddress Address { get; set; } = new();

    public TestMetrics Metrics { get; set; } = new();
}

public sealed class TestAddress
{
    public string City { get; set; } = string.Empty;

    public string PostalCode { get; set; } = string.Empty;
}

public sealed class TestMetrics
{
    public int Score { get; set; }

    public int Rank { get; set; }
}

public sealed class NestedProjectionResult
{
    public int Id { get; set; }

    public string City { get; set; } = string.Empty;

    public int Score { get; set; }
}
