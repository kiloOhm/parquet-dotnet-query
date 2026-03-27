# parquet-dotnet-query

`parquet-dotnet-query` is a small query layer for Parquet files built on top of [`kiloOhm.Parquet.Net`](https://www.nuget.org/packages/kiloOhm.Parquet.Net/).

It keeps Parquet-specific optimizations explicit:

- `Pushdown(...)` for predicates that should participate in parquet pruning
- `Where(...)` for normal LINQ predicates with residual in-memory evaluation
- `Select(...)` for projection and column pruning
- `FromFiles(...)` and `FromDirectory(...)` for dataset-style scans
- `PlanAsync()` and `ExplainAsync()` for visibility into what was pushed down

The current implementation focuses on file pruning, row-group pruning, and selective materialization rather than pretending to be a full LINQ provider.

## Features

- Explicit pushdown filter DSL
- Partial extraction from `Where(...)` expressions
- Statistics-based row-group pruning
- Partition-aware file pruning for directory layouts like `Country=DE/...`
- Bloom-filter-aware equality pruning when bloom filters are present
- Late materialization for projected queries
- Nested POCO materialization
- Nested projection with column pruning
- Residual predicate evaluation for unsupported logic
- Strict mode for "push down or fail"
- Encryption-friendly query options on top of `kiloOhm.Parquet.Net`

## Supported Pushdown

The pushdown subset is intentionally small and predictable.

Supported:

- Equality and inequality
- `<`, `<=`, `>`, `>=`
- `Between(...)`
- `StartsWith(..., StringComparison.Ordinal)`
- Conjunctive combinations with `&&`

Not pushed down:

- `||`
- arbitrary method calls
- culture-sensitive string operations
- complex computed expressions

Unsupported parts are still evaluated correctly as residual predicates after reading matching row groups.

## Example

```csharp
using Parquet.Query;

var rows = await ParquetQuery
    .FromDirectory<Person>("people")
    .Pushdown(filter => filter
        .Eq(row => row.Country, "DE")
        .Ge(row => row.Age, 18))
    .Where(row => row.Name.EndsWith("n"))
    .Select(row => new
    {
        row.Id,
        row.Name,
        City = row.Address.City
    })
    .ToListAsync();
```

Semantics:

- dataset queries can skip whole files based on partition values in the path
- `Pushdown(...)` is parquet-aware and plannable
- `Where(...)` can contain richer logic, but unsupported parts stay residual
- `Select(...)` drives projection and can reduce the columns read from the file
- projected queries can defer non-filter columns until after row filtering

## Planning And Diagnostics

Use `PlanAsync()` or `ExplainAsync()` to inspect what the query engine will do.

```csharp
var query = ParquetQuery
    .FromFile<Person>("people.parquet")
    .Where(row => row.Age >= 18 && row.Name.StartsWith("Lu", StringComparison.Ordinal));

var explanation = await query.ExplainAsync();
Console.WriteLine(explanation);
```

You will see:

- selected files
- extracted pushdown predicates
- residual predicates
- selected row groups
- filter columns versus deferred columns
- whether pruning was based on partitions, statistics, bloom filters, or schema fallback

If you want unsupported residual logic to fail fast instead of silently falling back, use:

```csharp
var rows = await ParquetQuery
    .FromFile<Person>("people.parquet")
    .Where(row => row.Country == "DE" && CustomCheck(row))
    .StrictPushdown()
    .ToListAsync();
```

## Encryption Support

The query layer forwards `ParquetOptions` and exposes convenience methods for common encrypted-read scenarios:

- `WithParquetOptions(...)`
- `ConfigureParquetOptions(...)`
- `WithFooterKey(...)`
- `WithFooterSigningKey(...)`
- `UsePlaintextFooter(...)`
- `WithAadPrefix(...)`
- `UseCtrVariant(...)`
- `WithColumnKeyResolver(...)`

Example:

```csharp
var rows = await ParquetQuery
    .FromFile<Person>("encrypted.parquet")
    .WithFooterKey("0123456789ABCDEF")
    .WithColumnKeyResolver((path, metadata) =>
    {
        if (path.Count > 0 && path[^1] == "Name")
        {
            return "0011223344556677";
        }

        return null;
    })
    .ToListAsync();
```

## Current Limits

- Partial materialization is aimed at nested class graphs with scalar leaves
- More complex shapes fall back to full source materialization
- Page-index metadata can be detected, but page-level pruning is not yet wired through the public `kiloOhm.Parquet.Net` reader API
- This is not a general-purpose `IQueryable` provider

That tradeoff is deliberate: the API stays storage-aware and keeps correctness simple.

## Development

Requirements:

- .NET 8 SDK

Restore, build, and test:

```powershell
dotnet build Parquet.Query.slnx
dotnet test Parquet.Query.slnx
```

## License

[MIT](LICENSE)
