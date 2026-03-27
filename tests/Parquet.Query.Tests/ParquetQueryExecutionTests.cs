using System.Text;
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
        Assert.Contains("Read Columns: Address/City, Metrics/Score", explanation);
        Assert.Contains("RG 0:", explanation);
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
        var keyMetadata = Encoding.UTF8.GetBytes("name-column-key");
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
            .WithFooterSigningKey("0011223344556677", Encoding.UTF8.GetBytes("footer-signing"))
            .UsePlaintextFooter()
            .WithAadPrefix("query-tests", supplyOutOfBand: true)
            .UseCtrVariant()
            .ConfigureParquetOptions(options => options.TreatByteArrayAsString = true)
            .WithFooterKey("8899AABBCCDDEEFF", Encoding.UTF8.GetBytes("footer-key"))
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
        Action<ParquetOptions>? configureOptions = null)
    {
        var parquetOptions = new ParquetOptions();
        parquetOptions.BloomFilterOptionsByColumn["Name"] = new ParquetOptions.BloomFilterOptions
        {
            EnableBloomFilters = true
        };
        configureOptions?.Invoke(parquetOptions);

        return ParquetSerializer.SerializeAsync(
            rows,
            filePath,
            new ParquetSerializerOptions
            {
                RowGroupSize = 2,
                ParquetOptions = parquetOptions
            });
    }

    private static Task WriteNestedRowsAsync(
        string filePath,
        IReadOnlyCollection<NestedTestRow> rows,
        Action<ParquetOptions>? configureOptions = null)
    {
        var parquetOptions = new ParquetOptions();
        configureOptions?.Invoke(parquetOptions);

        return ParquetSerializer.SerializeAsync(
            rows,
            filePath,
            new ParquetSerializerOptions
            {
                RowGroupSize = 2,
                ParquetOptions = parquetOptions
            });
    }

    private static T Identity<T>(T value) => value;
}
