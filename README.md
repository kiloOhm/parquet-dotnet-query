# parquet-dotnet-query

`parquet-dotnet-query` is a small query layer for Parquet files built on top of [`kiloOhm.Parquet.Net`](https://www.nuget.org/packages/kiloOhm.Parquet.Net/).

NuGet packages:

- `kiloOhm.Parquet.Net.Query` for the core query engine
- `kiloOhm.Parquet.Net.Query.Extensions.Writing` for write-side metadata helpers
- `kiloOhm.Parquet.Net.Query.Extensions.Indexing` for footer-backed equality indexes
- `kiloOhm.Parquet.Net.Query.Extensions.Search` for footer-backed text search
- `kiloOhm.Parquet.Net.Query.Extensions.Pooling` for reusable reader pooling
- `kiloOhm.Parquet.Net.Query.Extensions` as a convenience umbrella package for the full stack

It keeps Parquet-specific optimizations explicit:

- `Pushdown(...)` for predicates that should participate in parquet pruning
- `Where(...)` for normal LINQ predicates with residual in-memory evaluation
- `Select(...)` for projection and column pruning
- `FromFiles(...)` and `FromDirectory(...)` for dataset-style scans
- `PlanAsync()` and `ExplainAsync()` for visibility into what was pushed down

The current implementation focuses on file pruning, row-group pruning, page pruning, and selective materialization rather than pretending to be a full LINQ provider.

## Features

- Explicit pushdown filter DSL
- Partial extraction from `Where(...)` expressions
- Statistics-based row-group pruning
- Page-index-based page pruning within surviving row groups
- Partition-aware file pruning for directory layouts like `Country=DE/...`
- Bloom-filter-aware equality pruning when bloom filters are present
- Extensible predicate planners for custom footer or sidecar indexes
- Query-plan caching with a default bounded in-memory cache and pluggable custom caches
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
- selected pages and candidate-row upper bounds when page pruning applies
- filter columns versus deferred columns
- whether pruning was based on partitions, statistics, bloom filters, persisted page indexes, or fallback page-index scans
- page-pruning source per row group (`persisted`, `fallback`, or `unavailable`)

If you want unsupported residual logic to fail fast instead of silently falling back, use:

```csharp
var rows = await ParquetQuery
    .FromFile<Person>("people.parquet")
    .Where(row => row.Country == "DE" && CustomCheck(row))
    .StrictPushdown()
    .ToListAsync();
```

## Extensibility

Custom pushdown predicates can be added without forking the core query engine:

- create a custom `PushdownPredicate<T>`
- add it through `Pushdown(filter => filter.Add(...))`
- register one or more `IParquetPredicatePlanner<T>` instances with `WithPredicatePlanner(...)` or `WithPredicatePlanners(...)`

This lets extension packages use footer metadata, sidecar indexes, or custom row-group/page pruning logic while keeping residual row-level verification in the main query pipeline.

The repository now includes a search-focused extension project at `src/Parquet.Query.Extensions.Search` with:

- a `LuceneFooterIndexingStrategy` for `[ParquetLuceneIndex]` string columns
- footer-resident analyzed term dictionaries per row group
- `LuceneMatch(...)` and `LuceneFuzzy(...)` query extensions backed by custom predicate planning

It also now includes an indexing-focused extension project at `src/Parquet.Query.Extensions.Indexing` with:

- a `FooterBitmapIndexingStrategy` for `[ParquetFooterBitmapIndex]` low-cardinality equality columns
- `WithFooterIndexes()` query extensions backed by footer-aware equality pruning

It also now includes a pooling-focused extension project at `src/Parquet.Query.Extensions.Pooling` with:

- a `ParquetReaderPool` that reuses open readers per file
- `PrewarmAsync(...)` helpers so pools can be filled before queries arrive
- `BlockFileAsync(...)` helpers for coordinated file replacement
- `WithReaderPool()` query extensions that route query execution through the pool

If you want the full extension set in one install, use `kiloOhm.Parquet.Net.Query.Extensions`, which brings in the core query package plus writing, indexing, search, and pooling.

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

## Query Caching

Repeated queries automatically reuse cached planning metadata through a bounded in-memory cache in the core package.

You can disable it per query:

```csharp
var uncachedRows = await ParquetQuery
    .FromFile<Person>("people.parquet")
    .WithoutQueryCache()
    .Where(row => row.Country == "DE")
    .ToListAsync();
```

Or attach your own cache implementation:

```csharp
IParquetQueryCache cache = new LruParquetQueryCache(capacity: 512);

var rows = await ParquetQuery
    .FromDirectory<Person>("people")
    .WithQueryCache(cache)
    .Where(row => row.Country == "DE")
    .ToListAsync();
```

## Current Limits

- Partial materialization is aimed at nested class graphs with scalar leaves
- More complex shapes fall back to full source materialization
- Page pruning depends on the public page reader/index APIs in `kiloOhm.Parquet.Net`
- When persisted page indexes are absent, the query layer can fall back to computed in-memory column indexes for supported types
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

Run the included benchmarks:

```powershell
dotnet run -c Release --project benchmarks/Parquet.Query.Benchmarks/Parquet.Query.Benchmarks.csproj
```

The benchmark project compares:

- full-file deserialize plus in-memory filtering
- query execution with row-group pushdown
- query execution with page pruning and projection

## License

[MIT](LICENSE)
