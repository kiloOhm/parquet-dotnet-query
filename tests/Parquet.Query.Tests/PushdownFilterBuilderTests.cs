using Parquet.Query.Pushdown;

namespace Parquet.Query.Tests;

public sealed class PushdownFilterBuilderTests
{
    [Fact]
    public void Build_supports_all_declared_predicate_shapes()
    {
        var filter = global::Parquet.Query.Pushdown.Pushdown.For<TestRow>(builder => builder
            .Eq(row => row.Country, "DE")
            .NotEq(row => row.Name, "beta")
            .Lt(row => row.Age, 30)
            .Le(row => row.Age, 29)
            .Gt(row => row.Id, 0)
            .Ge(row => row.Id, 1)
            .Between(row => row.Age, 18, 65)
            .StartsWith(row => row.Name, "a"));

        Assert.Equal(9, filter.Predicates.Count);
        Assert.Equal(
            new[]
            {
                "Country == \"DE\"",
                "Name != \"beta\"",
                "Age < 30",
                "Age <= 29",
                "Id > 0",
                "Id >= 1",
                "Age >= 18",
                "Age <= 65",
                "Name.StartsWith(\"a\")"
            },
            filter.Predicates.Select(predicate => predicate.Description).ToArray());
    }
}
