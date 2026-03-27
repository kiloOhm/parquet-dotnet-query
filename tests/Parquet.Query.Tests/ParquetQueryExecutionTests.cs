using System.Linq.Expressions;
using System.Text;
using Parquet;
using Parquet.Meta;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;
using Parquet.Serialization;

namespace Parquet.Query.Tests;

public sealed class ParquetQueryExecutionTests : IAsyncLifetime
{
    private readonly string _tempDirectory = Path.Combine(Path.GetTempPath(), "Parquet.Query.Tests", Guid.NewGuid().ToString("N"));

    [Fact]
    public async Task ToListAsync_uses_statistics_for_where_pushdown()
    {
        var filePath = Path.Combine(_tempDirectory, "ages.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 1 },
                new TestRow { Id = 2, Country = "DE", Name = "beta", Age = 2 },
                new TestRow { Id = 3, Country = "DE", Name = "gamma", Age = 10 },
                new TestRow { Id = 4, Country = "DE", Name = "delta", Age = 20 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Age >= 10);

        var directRows = await ParquetSerializer.DeserializeAsync<TestRow>(filePath);
        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { 1, 2, 3, 4 }, directRows.Select(row => row.Id).ToArray());
        Assert.Equal(1, plan.RowGroups.Count(rowGroup => !rowGroup.ShouldRead));
        Assert.Equal(new[] { 3, 4 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task CountAsync_returns_exact_count_for_filtered_query_without_projection_materialization()
    {
        var filePath = Path.Combine(_tempDirectory, "count-filtered.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "bravo", Age = 20 },
                new TestRow { Id = 3, Country = "DE", Name = "charlie", Age = 30 },
                new TestRow { Id = 4, Country = "US", Name = "delta", Age = 40 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Country == "DE" && row.Age >= 20)
            .Select(row => row.Name);

        var count = await query.CountAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(2, count);
        Assert.Equal(rows.Count, count);
    }

    [Fact]
    public async Task CountAsync_uses_metadata_only_when_no_predicates_are_present()
    {
        var filePath = Path.Combine(_tempDirectory, "count-all.parquet");
        await WriteRowsAsync(
            filePath,
            Enumerable.Range(1, 6)
                .Select(index => new TestRow
                {
                    Id = index,
                    Country = index % 2 == 0 ? "DE" : "US",
                    Name = $"row-{index}",
                    Age = 20 + index
                })
                .ToArray(),
            configureSerializerOptions: options => options.RowGroupSize = 2);

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Select(row => row.Name);

        var plan = await query.PlanAsync();
        var count = await query.CountAsync();

        Assert.Equal(plan.RowGroups.Sum(rowGroup => rowGroup.RowCount), count);
        Assert.Equal(6, count);
    }

    [Fact]
    public async Task LongCountAsync_matches_CountAsync()
    {
        var filePath = Path.Combine(_tempDirectory, "long-count.parquet");
        await WriteRowsAsync(
            filePath,
            Enumerable.Range(1, 4)
                .Select(index => new TestRow
                {
                    Id = index,
                    Country = index <= 2 ? "DE" : "US",
                    Name = $"row-{index}",
                    Age = 10 * index
                })
                .ToArray());

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Country == "DE")
            .Select(row => row.Name);

        Assert.Equal(await query.CountAsync(), await query.LongCountAsync());
    }

    [Fact]
    public async Task AnyAsync_uses_metadata_shortcut_when_no_predicates_are_present()
    {
        var filePath = Path.Combine(_tempDirectory, "any-all.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 }
            });

        var any = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .AnyAsync();

        Assert.True(any);
    }

    [Fact]
    public async Task AnyAsync_returns_false_when_filtered_query_has_no_matches()
    {
        var filePath = Path.Combine(_tempDirectory, "any-none.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "beta", Age = 20 }
            });

        var any = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Country == "FR")
            .AnyAsync();

        Assert.False(any);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_returns_first_projected_match_without_materializing_all_results()
    {
        var filePath = Path.Combine(_tempDirectory, "first-or-default.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "bravo", Age = 20 },
                new TestRow { Id = 3, Country = "US", Name = "charlie", Age = 30 }
            });

        var first = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Country == "DE" && row.Age >= 20)
            .Select(row => row.Name)
            .FirstOrDefaultAsync();

        Assert.Equal("bravo", first);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_returns_default_when_no_rows_match()
    {
        var filePath = Path.Combine(_tempDirectory, "first-or-default-none.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 }
            });

        var first = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Country == "US")
            .Select(row => row.Name)
            .FirstOrDefaultAsync();

        Assert.Null(first);
    }

    [Fact]
    public async Task FirstOrDefaultAsync_supports_deferred_full_row_projection_path()
    {
        var filePath = Path.Combine(_tempDirectory, "first-or-default-collection.parquet");
        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new CollectionRow { Id = 1, Country = "DE", Tags = new[] { "a", "b" } },
                new CollectionRow { Id = 2, Country = "US", Tags = new[] { "c" } }
            },
            filePath);

        var first = await ParquetQuery
            .FromFile<CollectionRow>(filePath)
            .Where(row => row.Country == "DE")
            .Select(row => new CollectionProjectionResult
            {
                Id = row.Id,
                Tags = row.Tags
            })
            .FirstOrDefaultAsync();

        Assert.NotNull(first);
        Assert.Equal(1, first!.Id);
        Assert.Equal(new[] { "a", "b" }, first.Tags);
    }

    [Fact]
    public async Task ToListAsync_applies_residual_row_filter_after_pushdown()
    {
        var filePath = Path.Combine(_tempDirectory, "residual.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "bravo", Age = 12 },
                new TestRow { Id = 3, Country = "DE", Name = "charlie", Age = 14 },
                new TestRow { Id = 4, Country = "US", Name = "delta", Age = 16 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Age >= 12 && row.Name.EndsWith("o"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Single(plan.PushdownPredicates);
        Assert.Single(plan.ResidualPredicates);
        Assert.Single(rows);
        Assert.Equal("bravo", rows[0].Name);
    }

    [Fact]
    public async Task ToListAsync_materializes_nested_objects()
    {
        var filePath = Path.Combine(_tempDirectory, "nested.parquet");
        await WriteNestedRowsAsync(
            filePath,
            new[]
            {
                new NestedTestRow
                {
                    Id = 1,
                    Country = "DE",
                    Address = new TestAddress { City = "Berlin", PostalCode = "10115" },
                    Metrics = new TestMetrics { Score = 10, Rank = 1 }
                },
                new NestedTestRow
                {
                    Id = 2,
                    Country = "US",
                    Address = new TestAddress { City = "Boston", PostalCode = "02108" },
                    Metrics = new TestMetrics { Score = 5, Rank = 2 }
                }
            });

        var rows = await ParquetQuery
            .FromFile<NestedTestRow>(filePath)
            .Where(row => row.Address.City == "Berlin")
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal("Berlin", row.Address.City);
        Assert.Equal("10115", row.Address.PostalCode);
        Assert.Equal(10, row.Metrics.Score);
    }

    [Fact]
    public async Task Select_supports_projection_and_column_pruning_for_nested_members()
    {
        var filePath = Path.Combine(_tempDirectory, "projection.parquet");
        await WriteNestedRowsAsync(
            filePath,
            new[]
            {
                new NestedTestRow
                {
                    Id = 1,
                    Country = "DE",
                    Address = new TestAddress { City = "Berlin", PostalCode = "10115" },
                    Metrics = new TestMetrics { Score = 10, Rank = 1 }
                },
                new NestedTestRow
                {
                    Id = 2,
                    Country = "DE",
                    Address = new TestAddress { City = "Munich", PostalCode = "80331" },
                    Metrics = new TestMetrics { Score = 20, Rank = 2 }
                },
                new NestedTestRow
                {
                    Id = 3,
                    Country = "US",
                    Address = new TestAddress { City = "Boston", PostalCode = "02108" },
                    Metrics = new TestMetrics { Score = 30, Rank = 3 }
                }
            });

        var query = ParquetQuery
            .FromFile<NestedTestRow>(filePath)
            .Where(row => row.Country == "DE" && row.Metrics.Score >= 15)
            .Select(row => new NestedProjectionResult
            {
                Id = row.Id,
                City = row.Address.City,
                Score = row.Metrics.Score
            });

        var schema = await ParquetReader.ReadSchemaAsync(filePath);
        Assert.Equal(
            new[] { "Id", "Country", "Address/City", "Address/PostalCode", "Metrics/Score", "Metrics/Rank" },
            schema.GetDataFields().Select(field => field.Path.ToString()).ToArray());

        var projectionOnlyRows = await ParquetQuery
            .FromFile<NestedTestRow>(filePath)
            .Select(row => new NestedProjectionResult
            {
                Id = row.Id,
                City = row.Address.City,
                Score = row.Metrics.Score
            })
            .ToListAsync();

        Assert.Equal(3, projectionOnlyRows.Count);

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.False(plan.RequiresFullMaterialization);
        Assert.True(plan.UsesLateMaterialization);
        Assert.Equal(
            new[] { "Address/City", "Country", "Id", "Metrics/Score" },
            plan.ReadColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray());
        Assert.Equal(
            new[] { "Country", "Metrics/Score" },
            plan.FilterColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray());
        Assert.Equal(
            new[] { "Address/City", "Id" },
            plan.DeferredColumns.OrderBy(column => column, StringComparer.Ordinal).ToArray());

        var row = Assert.Single(rows);
        Assert.Equal(2, row.Id);
        Assert.Equal("Munich", row.City);
        Assert.Equal(20, row.Score);
    }

    [Fact]
    public async Task Select_supports_vectorized_constructor_projection()
    {
        var filePath = Path.Combine(_tempDirectory, "constructor-vectorized-projection.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "bravo", Age = 20 }
            });

        var rows = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Select(row => new ConstructorProjectionResult(row.Id, row.Name))
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal("alpha", rows[0].Name);
        Assert.Equal(2, rows[1].Id);
        Assert.Equal("bravo", rows[1].Name);
    }

    [Fact]
    public async Task Select_falls_back_to_full_materialization_for_parameter_based_projection()
    {
        var filePath = Path.Combine(_tempDirectory, "projection-full-materialization.parquet");
        await WriteNestedRowsAsync(
            filePath,
            new[]
            {
                new NestedTestRow
                {
                    Id = 1,
                    Country = "DE",
                    Address = new TestAddress { City = "Berlin", PostalCode = "10115" },
                    Metrics = new TestMetrics { Score = 10, Rank = 1 }
                },
                new NestedTestRow
                {
                    Id = 2,
                    Country = "US",
                    Address = new TestAddress { City = "Boston", PostalCode = "02108" },
                    Metrics = new TestMetrics { Score = 5, Rank = 2 }
                }
            });

        var query = ParquetQuery
            .FromFile<NestedTestRow>(filePath)
            .Select(row => Identity(row).Address.City);

        var plan = await query.PlanAsync();
        var cities = await query.ToListAsync();

        Assert.True(plan.RequiresFullMaterialization);
        Assert.Equal(new[] { "Berlin", "Boston" }, cities.ToArray());
    }

    [Fact]
    public async Task Equality_pushdown_returns_correct_rows()
    {
        var filePath = Path.Combine(_tempDirectory, "bloom.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "apple", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "carrot", Age = 11 },
                new TestRow { Id = 3, Country = "DE", Name = "banana", Age = 12 },
                new TestRow { Id = 4, Country = "DE", Name = "blueberry", Age = 13 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Eq(row => row.Name, "banana"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal("banana", row.Name);
        Assert.Contains(
            plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions),
            decision => decision.Source is "statistics" or "statistics+bloom" or "bloom");
    }

    [Fact]
    public async Task Pushdown_builder_and_statistics_prune_supported_comparison_operators()
    {
        var filePath = Path.Combine(_tempDirectory, "comparison-operators.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 5 },
                new TestRow { Id = 2, Country = "DE", Name = "bravo", Age = 5 },
                new TestRow { Id = 3, Country = "DE", Name = "charlie", Age = 6 },
                new TestRow { Id = 4, Country = "DE", Name = "delta", Age = 7 },
                new TestRow { Id = 5, Country = "DE", Name = "echo", Age = 8 },
                new TestRow { Id = 6, Country = "DE", Name = "foxtrot", Age = 9 }
            });

        var notEqualPlan = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.NotEq(row => row.Age, 5))
            .PlanAsync();

        var lessThanPlan = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Lt(row => row.Age, 6))
            .PlanAsync();

        var lessThanOrEqualPlan = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Le(row => row.Age, 5))
            .PlanAsync();

        var greaterThanPlan = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Gt(row => row.Age, 7))
            .PlanAsync();

        var greaterThanOrEqualPlan = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Ge(row => row.Age, 8))
            .PlanAsync();

        var betweenRows = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Between(row => row.Age, 6, 7))
            .ToListAsync();

        Assert.False(notEqualPlan.RowGroups[0].ShouldRead);
        Assert.False(lessThanPlan.RowGroups[1].ShouldRead);
        Assert.False(lessThanPlan.RowGroups[2].ShouldRead);
        Assert.False(lessThanOrEqualPlan.RowGroups[1].ShouldRead);
        Assert.False(lessThanOrEqualPlan.RowGroups[2].ShouldRead);
        Assert.False(greaterThanPlan.RowGroups[0].ShouldRead);
        Assert.False(greaterThanPlan.RowGroups[1].ShouldRead);
        Assert.False(greaterThanOrEqualPlan.RowGroups[0].ShouldRead);
        Assert.False(greaterThanOrEqualPlan.RowGroups[1].ShouldRead);
        Assert.Equal(new[] { 3, 4 }, betweenRows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task StartsWith_pushdown_uses_statistics()
    {
        var filePath = Path.Combine(_tempDirectory, "startswith.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "apricot", Age = 11 },
                new TestRow { Id = 3, Country = "DE", Name = "banana", Age = 12 },
                new TestRow { Id = 4, Country = "DE", Name = "blueberry", Age = 13 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.StartsWith(row => row.Name, "b"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.False(plan.RowGroups[0].ShouldRead);
        Assert.True(plan.RowGroups[1].ShouldRead);
        Assert.All(
            plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions),
            decision => Assert.Equal("statistics", decision.Source));
        Assert.Equal(new[] { "banana", "blueberry" }, rows.Select(row => row.Name).ToArray());
    }

    [Fact]
    public async Task ToAsyncEnumerable_applies_filters_and_projection()
    {
        var filePath = Path.Combine(_tempDirectory, "async-enumerable.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "beta", Age = 20 },
                new TestRow { Id = 3, Country = "US", Name = "gamma", Age = 30 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Country == "DE" && row.Age >= 15)
            .Select(row => row.Name);

        var names = new List<string>();
        await foreach (var name in query.ToAsyncEnumerable())
        {
            names.Add(name);
        }

        Assert.Equal(new[] { "beta" }, names.ToArray());
    }

    [Fact]
    public async Task ToAsyncEnumerable_uses_direct_scalar_projection_when_no_filters_are_present()
    {
        var filePath = Path.Combine(_tempDirectory, "async-direct-scalar.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "beta", Age = 20 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Select(row => row.Name);

        var names = new List<string>();
        await foreach (var name in query.ToAsyncEnumerable())
        {
            names.Add(name);
        }

        Assert.Equal(new[] { "alpha", "beta" }, names.ToArray());
    }

    [Fact]
    public async Task ToAsyncEnumerable_uses_vectorized_projection_for_member_init_results()
    {
        var filePath = Path.Combine(_tempDirectory, "async-vectorized-member-init.parquet");
        await WriteNestedRowsAsync(
            filePath,
            new[]
            {
                new NestedTestRow
                {
                    Id = 1,
                    Country = "DE",
                    Address = new TestAddress { City = "Berlin", PostalCode = "10115" },
                    Metrics = new TestMetrics { Score = 10, Rank = 1 }
                },
                new NestedTestRow
                {
                    Id = 2,
                    Country = "DE",
                    Address = new TestAddress { City = "Munich", PostalCode = "80331" },
                    Metrics = new TestMetrics { Score = 20, Rank = 2 }
                }
            });

        var query = ParquetQuery
            .FromFile<NestedTestRow>(filePath)
            .Where(row => row.Country == "DE" && row.Metrics.Score >= 15)
            .Select(row => new NestedProjectionResult
            {
                Id = row.Id,
                City = row.Address.City,
                Score = row.Metrics.Score
            });

        var results = new List<NestedProjectionResult>();
        await foreach (var result in query.ToAsyncEnumerable())
        {
            results.Add(result);
        }

        var row = Assert.Single(results);
        Assert.Equal(2, row.Id);
        Assert.Equal("Munich", row.City);
        Assert.Equal(20, row.Score);
    }

    [Fact]
    public async Task ToAsyncEnumerable_uses_vectorized_constructor_projection()
    {
        var filePath = Path.Combine(_tempDirectory, "async-vectorized-constructor.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "bravo", Age = 20 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Select(row => new ConstructorProjectionResult(row.Id, row.Name));

        var results = new List<ConstructorProjectionResult>();
        await foreach (var result in query.ToAsyncEnumerable())
        {
            results.Add(result);
        }

        Assert.Equal(2, results.Count);
        Assert.Equal("alpha", results[0].Name);
        Assert.Equal("bravo", results[1].Name);
    }

    [Fact]
    public async Task ToAsyncEnumerable_supports_deferred_full_row_collection_projection_path()
    {
        var filePath = Path.Combine(_tempDirectory, "async-collection-projection.parquet");
        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new CollectionRow { Id = 1, Country = "DE", Tags = new[] { "a", "b" } },
                new CollectionRow { Id = 2, Country = "US", Tags = new[] { "c" } }
            },
            filePath);

        var query = ParquetQuery
            .FromFile<CollectionRow>(filePath)
            .Where(row => row.Country == "DE")
            .Select(row => row.Tags);

        var rows = new List<string[]>();
        await foreach (var tags in query.ToAsyncEnumerable())
        {
            rows.Add(tags);
        }

        var result = Assert.Single(rows);
        Assert.Equal(new[] { "a", "b" }, result);
    }

    [Fact]
    public async Task ToAsyncEnumerable_supports_deferred_full_row_object_projection_path()
    {
        var filePath = Path.Combine(_tempDirectory, "async-collection-object-projection.parquet");
        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new CollectionRow { Id = 1, Country = "DE", Tags = new[] { "a", "b" } },
                new CollectionRow { Id = 2, Country = "US", Tags = new[] { "c" } }
            },
            filePath);

        var query = ParquetQuery
            .FromFile<CollectionRow>(filePath)
            .Where(row => row.Country == "DE")
            .Select(row => new CollectionProjectionResult
            {
                Id = row.Id,
                Tags = row.Tags
            });

        var rows = new List<CollectionProjectionResult>();
        await foreach (var result in query.ToAsyncEnumerable())
        {
            rows.Add(result);
        }

        var projection = Assert.Single(rows);
        Assert.Equal(1, projection.Id);
        Assert.Equal(new[] { "a", "b" }, projection.Tags);
    }

    [Fact]
    public async Task ToAsyncEnumerable_handles_full_materialization_projection_path()
    {
        var filePath = Path.Combine(_tempDirectory, "async-full-materialization.parquet");
        await WriteNestedRowsAsync(
            filePath,
            new[]
            {
                new NestedTestRow
                {
                    Id = 1,
                    Country = "DE",
                    Address = new TestAddress { City = "Berlin", PostalCode = "10115" },
                    Metrics = new TestMetrics { Score = 10, Rank = 1 }
                },
                new NestedTestRow
                {
                    Id = 2,
                    Country = "US",
                    Address = new TestAddress { City = "Boston", PostalCode = "02108" },
                    Metrics = new TestMetrics { Score = 20, Rank = 2 }
                }
            });

        var query = ParquetQuery
            .FromFile<NestedTestRow>(filePath)
            .Select(row => Identity(row).Address.City);

        var cities = new List<string>();
        await foreach (var city in query.ToAsyncEnumerable())
        {
            cities.Add(city);
        }

        Assert.Equal(new[] { "Berlin", "Boston" }, cities.ToArray());
    }

    [Fact]
    public async Task ToAsyncEnumerable_returns_empty_when_no_rows_match()
    {
        var filePath = Path.Combine(_tempDirectory, "async-empty.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "beta", Age = 20 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Ge(row => row.Age, 99))
            .Select(row => row.Name);

        var names = new List<string>();
        await foreach (var name in query.ToAsyncEnumerable())
        {
            names.Add(name);
        }

        Assert.Empty(names);
    }

    [Fact]
    public async Task ExplainAsync_reports_pushdown_residual_and_read_columns()
    {
        var filePath = Path.Combine(_tempDirectory, "explain.parquet");
        await WriteNestedRowsAsync(
            filePath,
            new[]
            {
                new NestedTestRow
                {
                    Id = 1,
                    Country = "DE",
                    Address = new TestAddress { City = "Berlin", PostalCode = "10115" },
                    Metrics = new TestMetrics { Score = 10, Rank = 1 }
                },
                new NestedTestRow
                {
                    Id = 2,
                    Country = "DE",
                    Address = new TestAddress { City = "Munich", PostalCode = "80331" },
                    Metrics = new TestMetrics { Score = 20, Rank = 2 }
                }
            });

        var explanation = await ParquetQuery
            .FromFile<NestedTestRow>(filePath)
            .Where(row => row.Metrics.Score >= 15 && row.Address.City.EndsWith("h"))
            .Select(row => row.Address.City)
            .ExplainAsync();

        Assert.Contains("Pushdown: Metrics.Score >= 15", explanation);
        Assert.Contains("Residual: row.Address.City.EndsWith(\"h\")", explanation);
        Assert.Contains("Residual Diagnostics:", explanation);
        Assert.Contains("Method calls are not pushdown-eligible", explanation);
        Assert.Contains("Read Columns: Address/City, Metrics/Score", explanation);
        Assert.Contains("RG 0:", explanation);
    }

    [Fact]
    public async Task Select_supports_collection_leaf_projection_without_full_materialization()
    {
        var filePath = Path.Combine(_tempDirectory, "collection-projection.parquet");
        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new CollectionRow { Id = 1, Country = "DE", Tags = new[] { "a", "b" } },
                new CollectionRow { Id = 2, Country = "US", Tags = new[] { "c" } }
            },
            filePath);

        var query = ParquetQuery
            .FromFile<CollectionRow>(filePath)
            .Where(row => row.Country == "DE")
            .Select(row => row.Tags);

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.False(plan.RequiresFullMaterialization);
        var tags = Assert.Single(rows);
        Assert.Equal(new[] { "a", "b" }, tags);
    }

    [Fact]
    public async Task Select_supports_projection_of_filter_column_via_direct_scalar_path()
    {
        var filePath = Path.Combine(_tempDirectory, "filter-column-projection.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "bravo", Age = 20 },
                new TestRow { Id = 3, Country = "US", Name = "charlie", Age = 30 }
            });

        var rows = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Name.StartsWith("b", StringComparison.Ordinal))
            .Select(row => row.Name)
            .ToListAsync();

        Assert.Equal(new[] { "bravo" }, rows.ToArray());
    }

    [Fact]
    public async Task Select_supports_collection_leaf_projection_without_filters()
    {
        var filePath = Path.Combine(_tempDirectory, "collection-projection-no-filter.parquet");
        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new CollectionRow { Id = 1, Country = "DE", Tags = new[] { "a", "b" } },
                new CollectionRow { Id = 2, Country = "US", Tags = new[] { "c" } }
            },
            filePath);

        var rows = await ParquetQuery
            .FromFile<CollectionRow>(filePath)
            .Select(row => row.Tags)
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.Equal(new[] { "a", "b" }, rows[0]);
        Assert.Equal(new[] { "c" }, rows[1]);
    }

    [Fact]
    public async Task PlanAsync_supports_list_collection_leaf_projection_without_full_materialization()
    {
        var filePath = Path.Combine(_tempDirectory, "list-collection-projection.parquet");
        await ParquetSerializer.SerializeAsync(
            new[]
            {
                new ListCollectionRow { Id = 1, Country = "DE", Tags = new List<string> { "a", "b" } },
                new ListCollectionRow { Id = 2, Country = "US", Tags = new List<string> { "c" } }
            },
            filePath);

        var query = ParquetQuery
            .FromFile<ListCollectionRow>(filePath)
            .Where(row => row.Country == "DE")
            .Select(row => row.Tags);

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.False(plan.RequiresFullMaterialization);
        var tags = Assert.Single(rows);
        Assert.Equal(new[] { "a", "b" }, tags);
    }

    [Fact]
    public async Task ExplainAsync_reports_page_pruning_source_and_counts()
    {
        var filePath = Path.Combine(_tempDirectory, "explain-page-pruning.parquet");
        await WriteRowsAsync(
            filePath,
            Enumerable.Range(1, 6)
                .Select(age => new TestRow { Id = age, Country = "DE", Name = $"row-{age}", Age = age })
                .ToArray(),
            configureOptions: options => options.DataPageRowCountLimit = 2,
            configureSerializerOptions: options => options.RowGroupSize = 6);

        var explanation = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Ge(row => row.Age, 5))
            .ExplainAsync();

        Assert.Contains("Page Indexes: available", explanation);
        Assert.Contains("pages 1/3", explanation);
        Assert.Contains("page pruning: persisted", explanation);
    }

    [Fact]
    public async Task PlanAsync_reports_schema_miss_for_unknown_pushdown_column()
    {
        var filePath = Path.Combine(_tempDirectory, "schema-miss.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "beta", Age = 11 }
            });

        var plan = await ParquetQuery
            .FromFile<SchemaMismatchRow>(filePath)
            .Pushdown(filter => filter.Eq(row => row.Missing, 42))
            .PlanAsync();

        var decision = Assert.Single(plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions));
        Assert.Equal("schema", decision.Source);
        Assert.True(decision.MayMatch);
    }

    [Fact]
    public async Task FromDirectory_prunes_partitioned_files_before_opening_them()
    {
        var rootPath = Path.Combine(_tempDirectory, "dataset");
        var germanyPath = Path.Combine(rootPath, "Country=DE", "de.parquet");
        var usaPath = Path.Combine(rootPath, "Country=US", "us.parquet");

        Directory.CreateDirectory(Path.GetDirectoryName(germanyPath)!);
        Directory.CreateDirectory(Path.GetDirectoryName(usaPath)!);

        await WriteRowsAsync(
            germanyPath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "bravo", Age = 20 }
            });

        await WriteRowsAsync(
            usaPath,
            new[]
            {
                new TestRow { Id = 3, Country = "US", Name = "charlie", Age = 30 },
                new TestRow { Id = 4, Country = "US", Name = "delta", Age = 40 }
            });

        var query = ParquetQuery
            .FromDirectory<TestRow>(rootPath)
            .Pushdown(filter => filter.Eq(row => row.Country, "DE"));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(2, plan.Files.Count);
        Assert.Equal(1, plan.SelectedFileCount);

        var skippedFile = Assert.Single(plan.Files.Where(file => !file.ShouldRead));
        Assert.Contains("Country=US", skippedFile.FilePath);
        var partitionDecision = Assert.Single(skippedFile.Decisions);
        Assert.Equal("partition", partitionDecision.Source);
        Assert.False(partitionDecision.MayMatch);

        Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task PlanAsync_uses_page_indexes_to_prune_single_row_group_pages()
    {
        var filePath = Path.Combine(_tempDirectory, "page-pruning.parquet");
        await WriteRowsAsync(
            filePath,
            Enumerable.Range(1, 6)
                .Select(age => new TestRow { Id = age, Country = "DE", Name = $"row-{age}", Age = age })
                .ToArray(),
            configureOptions: options => options.DataPageRowCountLimit = 2,
            configureSerializerOptions: options => options.RowGroupSize = 6);

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Ge(row => row.Age, 5));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        var rowGroup = Assert.Single(plan.RowGroups);
        Assert.False(plan.RequiresFullMaterialization);
        Assert.True(plan.PageIndexAvailable);
        Assert.Equal(3, rowGroup.PageCount);
        Assert.Equal(1, rowGroup.SelectedPageCount);
        Assert.Equal(2, rowGroup.CandidateRowCountUpperBound);
        Assert.Equal(new[] { 5, 6 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task PlanAsync_falls_back_to_scanned_page_indexes_when_footer_indexes_are_missing()
    {
        var filePath = Path.Combine(_tempDirectory, "page-pruning-fallback.parquet");
        await WriteRowsAsync(
            filePath,
            Enumerable.Range(1, 6)
                .Select(age => new TestRow { Id = age, Country = "DE", Name = $"row-{age}", Age = age })
                .ToArray(),
            configureOptions: options => options.DataPageRowCountLimit = 2,
            configureSerializerOptions: options => options.RowGroupSize = 6);

        await RemovePageIndexesFromFooterAsync(filePath);

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Ge(row => row.Age, 5));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        var rowGroup = Assert.Single(plan.RowGroups);
        Assert.True(plan.PageIndexAvailable);
        Assert.True(rowGroup.UsedFallbackPageIndex);
        Assert.Equal(3, rowGroup.PageCount);
        Assert.Equal(1, rowGroup.SelectedPageCount);
        Assert.Equal(2, rowGroup.CandidateRowCountUpperBound);
        Assert.Equal(new[] { 5, 6 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task Custom_predicate_planners_can_prune_files_from_footer_metadata()
    {
        var deFilePath = Path.Combine(_tempDirectory, "footer-de.parquet");
        var usFilePath = Path.Combine(_tempDirectory, "footer-us.parquet");

        await WriteRowsAsync(
            deFilePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "DE", Name = "bravo", Age = 20 }
            });
        await WriteRowsAsync(
            usFilePath,
            new[]
            {
                new TestRow { Id = 3, Country = "US", Name = "charlie", Age = 30 },
                new TestRow { Id = 4, Country = "US", Name = "delta", Age = 40 }
            });

        await SetFileMetadataAsync(deFilePath, new Dictionary<string, string> { ["country"] = "DE" });
        await SetFileMetadataAsync(usFilePath, new Dictionary<string, string> { ["country"] = "US" });

        var query = ParquetQuery
            .FromFiles<TestRow>(new[] { deFilePath, usFilePath })
            .WithPredicatePlanner(new FooterMetadataPredicatePlanner<TestRow>())
            .Pushdown(filter => filter.Add(new FooterMetadataEqualsPredicate<TestRow>(row => row.Country, "country", "DE")));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id).ToArray());
        Assert.Equal(1, plan.Files.Count(file => file.ShouldRead));
        Assert.Contains(
            plan.RowGroups.SelectMany(rowGroup => rowGroup.Decisions),
            decision => decision.Source == "footer-metadata" && !decision.MayMatch);
    }

    [Fact]
    public async Task ToListAsync_without_projection_uses_page_pruning_for_simple_rows()
    {
        var filePath = Path.Combine(_tempDirectory, "full-row-page-pruning.parquet");
        await WriteRowsAsync(
            filePath,
            Enumerable.Range(1, 6)
                .Select(age => new TestRow { Id = age, Country = "DE", Name = $"row-{age}", Age = age })
                .ToArray(),
            configureOptions: options => options.DataPageRowCountLimit = 2,
            configureSerializerOptions: options => options.RowGroupSize = 6);

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Pushdown(filter => filter.Ge(row => row.Age, 5));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        var rowGroup = Assert.Single(plan.RowGroups);
        Assert.False(plan.RequiresFullMaterialization);
        Assert.Equal(3, rowGroup.PageCount);
        Assert.Equal(1, rowGroup.SelectedPageCount);
        Assert.Equal(new[] { 5, 6 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task NotEqual_pushdown_keeps_null_only_pages_in_play()
    {
        var filePath = Path.Combine(_tempDirectory, "nullable-page-pruning.parquet");
        await WriteGenericRowsAsync(
            filePath,
            new[]
            {
                new NullableScoreRow { Id = 1, Score = null },
                new NullableScoreRow { Id = 2, Score = null },
                new NullableScoreRow { Id = 3, Score = 5 },
                new NullableScoreRow { Id = 4, Score = 6 }
            },
            configureOptions: options => options.DataPageRowCountLimit = 2,
            configureSerializerOptions: options => options.RowGroupSize = 4);

        var query = ParquetQuery
            .FromFile<NullableScoreRow>(filePath)
            .Pushdown(filter => filter.NotEq(row => row.Score, 5));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        var rowGroup = Assert.Single(plan.RowGroups);
        Assert.Equal(2, rowGroup.PageCount);
        Assert.Equal(2, rowGroup.SelectedPageCount);
        Assert.Equal(new[] { 1, 2, 4 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task Bool_predicates_without_column_index_support_keep_full_page_coverage()
    {
        var filePath = Path.Combine(_tempDirectory, "bool-no-column-index.parquet");
        await WriteGenericRowsAsync(
            filePath,
            new[]
            {
                new BoolFlagRow { Id = 1, Flag = false },
                new BoolFlagRow { Id = 2, Flag = false },
                new BoolFlagRow { Id = 3, Flag = true },
                new BoolFlagRow { Id = 4, Flag = true }
            },
            configureOptions: options => options.DataPageRowCountLimit = 2,
            configureSerializerOptions: options => options.RowGroupSize = 4);

        var query = ParquetQuery
            .FromFile<BoolFlagRow>(filePath)
            .Pushdown(filter => filter.Eq(row => row.Flag, true));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        var rowGroup = Assert.Single(plan.RowGroups);
        Assert.True(rowGroup.PageIndexAvailable);
        Assert.Equal(0, rowGroup.SelectedPageCount);
        Assert.Equal(rowGroup.RowCount, rowGroup.CandidateRowCountUpperBound);
        Assert.Equal(new[] { 3, 4 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task PlanAsync_falls_back_to_full_materialization_for_recursive_source_shapes()
    {
        var filePath = Path.Combine(_tempDirectory, "recursive-shape.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 }
            });

        var plan = await ParquetQuery
            .FromFile<RecursiveMaterializationRow>(filePath)
            .PlanAsync();

        Assert.True(plan.RequiresFullMaterialization);
    }

    [Fact]
    public async Task WithFooterKey_reads_encrypted_footer_file()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-encrypted.parquet");
        const string footerKey = "0123456789ABCDEF";
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "beta", Age = 11 }
            },
            options =>
            {
                options.FooterEncryptionKey = footerKey;
            });

        await Assert.ThrowsAnyAsync<Exception>(() =>
            ParquetQuery
                .FromFile<TestRow>(filePath)
                .ToListAsync());

        var rows = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .WithFooterKey(footerKey)
            .ToListAsync();

        Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task WithColumnKeyResolver_reads_column_encrypted_file()
    {
        var filePath = Path.Combine(_tempDirectory, "column-encrypted.parquet");
        var keyMetadata = System.Text.Encoding.UTF8.GetBytes("name-column-key");
        const string footerKey = "FEDCBA9876543210";
        const string columnKey = "0011223344556677";

        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "beta", Age = 11 }
            },
            options =>
            {
                options.FooterEncryptionKey = footerKey;
                options.ColumnKeys["Name"] = new ParquetOptions.ColumnKeySpec(columnKey, keyMetadata);
            });

        await Assert.ThrowsAnyAsync<Exception>(() =>
            ParquetQuery
                .FromFile<TestRow>(filePath)
                .WithFooterKey(footerKey)
                .Select(row => row.Name)
                .ToListAsync());

        var names = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .WithFooterKey(footerKey)
            .WithColumnKeyResolver((path, metadata) =>
            {
                if (path.Count > 0 &&
                    string.Equals(path[^1], "Name", StringComparison.Ordinal) &&
                    metadata is not null &&
                    metadata.SequenceEqual(keyMetadata))
                {
                    return columnKey;
                }

                return null;
            })
            .Select(row => row.Name)
            .ToListAsync();

        Assert.Equal(new[] { "alpha", "beta" }, names.ToArray());
    }

    [Fact]
    public async Task Footer_encrypted_page_indexes_support_page_pruning()
    {
        var filePath = Path.Combine(_tempDirectory, "footer-encrypted-page-indexes.parquet");
        const string footerKey = "0123456789ABCDEF";

        await WriteRowsAsync(
            filePath,
            Enumerable.Range(1, 6)
                .Select(age => new TestRow { Id = age, Country = "DE", Name = $"row-{age}", Age = age })
                .ToArray(),
            options =>
            {
                options.FooterEncryptionKey = footerKey;
                options.DataPageRowCountLimit = 2;
            },
            serializer => serializer.RowGroupSize = 6);

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .WithFooterKey(footerKey)
            .Pushdown(filter => filter.Ge(row => row.Age, 5));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        var rowGroup = Assert.Single(plan.RowGroups);
        Assert.True(plan.PageIndexAvailable);
        Assert.Equal("persisted", rowGroup.PagePruningSource);
        Assert.Equal(1, rowGroup.SelectedPageCount);
        Assert.Equal(new[] { 5, 6 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task Column_encrypted_page_indexes_support_pruning_with_key_resolver()
    {
        var filePath = Path.Combine(_tempDirectory, "column-encrypted-page-indexes.parquet");
        const string footerKey = "FEDCBA9876543210";
        const string columnKey = "0011223344556677";
        var keyMetadata = System.Text.Encoding.UTF8.GetBytes("age-column-key");

        await WriteRowsAsync(
            filePath,
            Enumerable.Range(1, 6)
                .Select(age => new TestRow { Id = age, Country = "DE", Name = $"row-{age}", Age = age })
                .ToArray(),
            options =>
            {
                options.FooterEncryptionKey = footerKey;
                options.ColumnKeys["Age"] = new ParquetOptions.ColumnKeySpec(columnKey, keyMetadata);
                options.DataPageRowCountLimit = 2;
            },
            serializer => serializer.RowGroupSize = 6);

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .WithFooterKey(footerKey)
            .WithColumnKeyResolver((path, metadata) =>
            {
                if (path.Count > 0 &&
                    string.Equals(path[^1], "Age", StringComparison.Ordinal) &&
                    metadata is not null &&
                    metadata.SequenceEqual(keyMetadata))
                {
                    return columnKey;
                }

                return null;
            })
            .Pushdown(filter => filter.Ge(row => row.Age, 5));

        var plan = await query.PlanAsync();
        var rows = await query.ToListAsync();

        var rowGroup = Assert.Single(plan.RowGroups);
        Assert.True(plan.PageIndexAvailable);
        Assert.Equal(1, rowGroup.SelectedPageCount);
        Assert.Equal(new[] { 5, 6 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task Encryption_option_helpers_do_not_break_plain_reads()
    {
        var filePath = Path.Combine(_tempDirectory, "options-helpers.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "beta", Age = 11 }
            });

        var rows = await ParquetQuery
            .FromFile<TestRow>(filePath)
            .WithFooterSigningKey("0011223344556677", System.Text.Encoding.UTF8.GetBytes("footer-signing"))
            .UsePlaintextFooter()
            .WithAadPrefix("query-tests", supplyOutOfBand: true)
            .UseCtrVariant()
            .ConfigureParquetOptions(options => options.TreatByteArrayAsString = true)
            .WithFooterKey("8899AABBCCDDEEFF", System.Text.Encoding.UTF8.GetBytes("footer-key"))
            .ToListAsync();

        Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task WithParquetOptions_clones_the_supplied_options()
    {
        var filePath = Path.Combine(_tempDirectory, "cloned-options.parquet");
        const string footerKey = "0123456789ABCDEF";
        var options = new ParquetOptions
        {
            FooterEncryptionKey = footerKey
        };

        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "alpha", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "beta", Age = 11 }
            },
            writeOptions =>
            {
                writeOptions.FooterEncryptionKey = footerKey;
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .WithParquetOptions(options);

        options.FooterEncryptionKey = "FFFFFFFFFFFFFFFF";

        var rows = await query.ToListAsync();

        Assert.Equal(new[] { 1, 2 }, rows.Select(row => row.Id).ToArray());
    }

    [Fact]
    public async Task StrictPushdown_throws_when_where_contains_residual_logic()
    {
        var filePath = Path.Combine(_tempDirectory, "strict.parquet");
        await WriteRowsAsync(
            filePath,
            new[]
            {
                new TestRow { Id = 1, Country = "DE", Name = "apple", Age = 10 },
                new TestRow { Id = 2, Country = "US", Name = "banana", Age = 11 }
            });

        var query = ParquetQuery
            .FromFile<TestRow>(filePath)
            .Where(row => row.Country == "DE" && row.Name.EndsWith("e"))
            .StrictPushdown();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => query.PlanAsync());
        Assert.Contains("EndsWith", exception.Message);
    }

    public Task InitializeAsync()
    {
        Directory.CreateDirectory(_tempDirectory);
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        if (Directory.Exists(_tempDirectory))
        {
            Directory.Delete(_tempDirectory, recursive: true);
        }

        return Task.CompletedTask;
    }

    private static Task WriteRowsAsync(
        string filePath,
        IReadOnlyCollection<TestRow> rows,
        Action<ParquetOptions>? configureOptions = null,
        Action<ParquetSerializerOptions>? configureSerializerOptions = null)
    {
        var parquetOptions = new ParquetOptions();
        parquetOptions.BloomFilterOptionsByColumn["Name"] = new ParquetOptions.BloomFilterOptions
        {
            EnableBloomFilters = true
        };
        configureOptions?.Invoke(parquetOptions);

        var serializerOptions = new ParquetSerializerOptions
        {
            RowGroupSize = 2,
            ParquetOptions = parquetOptions
        };
        configureSerializerOptions?.Invoke(serializerOptions);

        return ParquetSerializer.SerializeAsync(
            rows,
            filePath,
            serializerOptions);
    }

    private static Task WriteGenericRowsAsync<TRow>(
        string filePath,
        IReadOnlyCollection<TRow> rows,
        Action<ParquetOptions>? configureOptions = null,
        Action<ParquetSerializerOptions>? configureSerializerOptions = null)
        where TRow : class, new()
    {
        var parquetOptions = new ParquetOptions();
        configureOptions?.Invoke(parquetOptions);

        var serializerOptions = new ParquetSerializerOptions
        {
            RowGroupSize = 2,
            ParquetOptions = parquetOptions
        };
        configureSerializerOptions?.Invoke(serializerOptions);

        return ParquetSerializer.SerializeAsync(
            rows,
            filePath,
            serializerOptions);
    }

    private static Task WriteNestedRowsAsync(
        string filePath,
        IReadOnlyCollection<NestedTestRow> rows,
        Action<ParquetOptions>? configureOptions = null,
        Action<ParquetSerializerOptions>? configureSerializerOptions = null)
    {
        var parquetOptions = new ParquetOptions();
        configureOptions?.Invoke(parquetOptions);

        var serializerOptions = new ParquetSerializerOptions
        {
            RowGroupSize = 2,
            ParquetOptions = parquetOptions
        };
        configureSerializerOptions?.Invoke(serializerOptions);

        return ParquetSerializer.SerializeAsync(
            rows,
            filePath,
            serializerOptions);
    }

    private static T Identity<T>(T value) => value;

    private static async Task SetFileMetadataAsync(string filePath, IReadOnlyDictionary<string, string> metadata)
    {
        var fileBytes = await System.IO.File.ReadAllBytesAsync(filePath);

        using var input = new MemoryStream(fileBytes, writable: false);
        using var reader = await ParquetReader.CreateAsync(input);

        FileMetaData footerMetadata = reader.Metadata!;
        footerMetadata.KeyValueMetadata = reader.CustomMetadata
            .Concat(metadata)
            .GroupBy(entry => entry.Key, StringComparer.Ordinal)
            .Select(group => new KeyValue { Key = group.Key, Value = group.Last().Value })
            .ToList();

        var originalFooterLength = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
        var footerStart = fileBytes.Length - 8 - originalFooterLength;

        using var output = new MemoryStream();
        await output.WriteAsync(fileBytes.AsMemory(0, footerStart));

        var footerType = typeof(ParquetReader).Assembly.GetType("Parquet.File.ThriftFooter", throwOnError: true)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be loaded.");
        var footer = Activator.CreateInstance(footerType, footerMetadata)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be created.");
        var writeMethod = footerType.GetMethod(
            "Write",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic,
            binder: null,
            types: new[] { typeof(Stream) },
            modifiers: null)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter.Write(Stream) could not be found.");
        var newFooterLength = checked(Convert.ToInt32(writeMethod.Invoke(footer, new object[] { output })));
        await output.WriteAsync(BitConverter.GetBytes(newFooterLength));
        await output.WriteAsync(System.Text.Encoding.ASCII.GetBytes("PAR1"));

        await System.IO.File.WriteAllBytesAsync(filePath, output.ToArray());
    }

    private static async Task RemovePageIndexesFromFooterAsync(string filePath)
    {
        var fileBytes = await System.IO.File.ReadAllBytesAsync(filePath);

        using var input = new MemoryStream(fileBytes, writable: false);
        using var reader = await ParquetReader.CreateAsync(input);

        FileMetaData metadata = reader.Metadata!;
        foreach (var rowGroup in metadata.RowGroups)
        {
            foreach (var columnChunk in rowGroup.Columns)
            {
                columnChunk.OffsetIndexOffset = null;
                columnChunk.OffsetIndexLength = null;
                columnChunk.ColumnIndexOffset = null;
                columnChunk.ColumnIndexLength = null;
            }
        }

        var originalFooterLength = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
        var footerStart = fileBytes.Length - 8 - originalFooterLength;

        using var output = new MemoryStream();
        await output.WriteAsync(fileBytes.AsMemory(0, footerStart));

        var footerType = typeof(ParquetReader).Assembly.GetType("Parquet.File.ThriftFooter", throwOnError: true)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be loaded.");
        var footer = Activator.CreateInstance(footerType, metadata)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter could not be created.");
        var writeMethod = footerType.GetMethod(
            "Write",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic,
            binder: null,
            types: new[] { typeof(Stream) },
            modifiers: null)
            ?? throw new InvalidOperationException("Parquet.File.ThriftFooter.Write(Stream) could not be found.");
        var newFooterLength = checked(Convert.ToInt32(writeMethod.Invoke(footer, new object[] { output })));
        await output.WriteAsync(BitConverter.GetBytes(newFooterLength));
        await output.WriteAsync(System.Text.Encoding.ASCII.GetBytes("PAR1"));

        await System.IO.File.WriteAllBytesAsync(filePath, output.ToArray());
    }

    private sealed class FooterMetadataEqualsPredicate<T> : PushdownPredicate<T>
        where T : class, new()
    {
        public FooterMetadataEqualsPredicate(
            Expression<Func<T, string?>> selector,
            string metadataKey,
            string expectedValue)
            : base(
                PushdownColumnPath.Resolve(selector).MemberPath,
                PushdownColumnPath.Resolve(selector).ColumnPath,
                $"footer[{metadataKey}] == \"{expectedValue}\"",
                CreateRowPredicate(selector, expectedValue))
        {
            MetadataKey = metadataKey;
            ExpectedValue = expectedValue;
        }

        public string MetadataKey { get; }

        public string ExpectedValue { get; }

        private static Func<T, bool> CreateRowPredicate(Expression<Func<T, string?>> selector, string expectedValue)
        {
            var compiledSelector = selector.Compile();
            return row => string.Equals(compiledSelector(row), expectedValue, StringComparison.Ordinal);
        }
    }

    private sealed class FooterMetadataPredicatePlanner<T> : IParquetPredicatePlanner<T>
        where T : class, new()
    {
        public bool CanPlan(PushdownPredicate<T> predicate) => predicate is FooterMetadataEqualsPredicate<T>;

        public RowGroupPredicateDecision? TryEvaluateRowGroup(
            ParquetRowGroupPlannerContext context,
            PushdownPredicate<T> predicate)
        {
            if (predicate is not FooterMetadataEqualsPredicate<T> footerPredicate)
            {
                return null;
            }

            var mayMatch = context.Reader.CustomMetadata.TryGetValue(footerPredicate.MetadataKey, out var actualValue) &&
                string.Equals(actualValue, footerPredicate.ExpectedValue, StringComparison.Ordinal);

            return new RowGroupPredicateDecision(
                predicate.Description,
                mayMatch,
                "footer-metadata",
                mayMatch
                    ? $"Footer metadata '{footerPredicate.MetadataKey}' matched '{footerPredicate.ExpectedValue}'."
                    : $"Footer metadata '{footerPredicate.MetadataKey}' did not match '{footerPredicate.ExpectedValue}'.");
        }

        public ValueTask<PagePruningResult?> TryPrunePagesAsync(
            ParquetPagePruningContext context,
            PushdownPredicate<T> predicate,
            CancellationToken cancellationToken = default)
            => ValueTask.FromResult<PagePruningResult?>(null);
    }
}
