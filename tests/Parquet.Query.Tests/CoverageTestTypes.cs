namespace Parquet.Query.Tests;

public sealed class SchemaMismatchRow
{
    public int Id { get; set; }

    public string Country { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public int Age { get; set; }

    public int Missing { get; set; }
}
