namespace Parquet.Query.Tests;

public sealed class SchemaMismatchRow
{
    public int Id { get; set; }

    public string Country { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public int Age { get; set; }

    public int Missing { get; set; }
}

public sealed class NullableScoreRow
{
    public int Id { get; set; }

    public int? Score { get; set; }
}

public sealed class BoolFlagRow
{
    public int Id { get; set; }

    public bool Flag { get; set; }
}

public sealed class RecursiveMaterializationRow
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public RecursiveMaterializationRow? Child { get; set; }
}

public sealed class ReadOnlyPropertyRow
{
    public int Id { get; init; }

    public string Name { get; } = string.Empty;
}

public sealed class ComplexPredicateRow
{
    public int Id { get; set; }

    public TestAddress Address { get; set; } = new();
}

public sealed class NoDefaultCtorNestedRow
{
    public int Id { get; set; }

    public NoDefaultCtorAddress Address { get; set; } = new("unknown");
}

public sealed class NoDefaultCtorAddress
{
    public NoDefaultCtorAddress(string city)
    {
        City = city;
    }

    public string City { get; }
}
