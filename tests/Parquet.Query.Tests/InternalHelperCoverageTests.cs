using System.Collections.Concurrent;
using System.Linq.Expressions;
using Parquet.Query.Extensions.Writing;
using Parquet.Query.Internal;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Tests;

public sealed class InternalHelperCoverageTests
{
    [Fact]
    public void BoundedPlannerCache_set_keeps_entries_when_within_limit()
    {
        var cache = new ConcurrentDictionary<string, CacheEntry>();

        BoundedPlannerCache.Set(cache, "first", new CacheEntry(10), maxEntries: 2, entry => entry.AccessTicks);
        BoundedPlannerCache.Set(cache, "second", new CacheEntry(20), maxEntries: 2, entry => entry.AccessTicks);

        Assert.Equal(2, cache.Count);
        Assert.Contains("first", cache.Keys);
        Assert.Contains("second", cache.Keys);
    }

    [Fact]
    public void BoundedPlannerCache_set_evicts_oldest_entries_when_limit_is_exceeded()
    {
        var cache = new ConcurrentDictionary<string, CacheEntry>();

        BoundedPlannerCache.Set(cache, "oldest", new CacheEntry(10), maxEntries: 2, entry => entry.AccessTicks);
        BoundedPlannerCache.Set(cache, "middle", new CacheEntry(20), maxEntries: 2, entry => entry.AccessTicks);
        BoundedPlannerCache.Set(cache, "newest", new CacheEntry(30), maxEntries: 2, entry => entry.AccessTicks);

        Assert.Equal(2, cache.Count);
        Assert.DoesNotContain("oldest", cache.Keys);
        Assert.Contains("middle", cache.Keys);
        Assert.Contains("newest", cache.Keys);
    }

    [Fact]
    public void PushdownPredicateFactory_converts_and_formats_supported_values()
    {
        var timestamp = new DateTime(2024, 01, 02, 03, 04, 05, DateTimeKind.Utc);

        Assert.Equal(SampleStatus.Done, PushdownPredicateFactory.ConvertValue("Done", typeof(SampleStatus)));
        Assert.Equal(SampleStatus.New, PushdownPredicateFactory.ConvertValue(1, typeof(SampleStatus)));
        Assert.Equal(42, PushdownPredicateFactory.ConvertValue("42", typeof(int)));
        Assert.Equal(timestamp, PushdownPredicateFactory.ConvertValue(timestamp, typeof(DateTime)));

        Assert.Equal("null", PushdownPredicateFactory.FormatValue(null));
        Assert.Equal("\"hello\"", PushdownPredicateFactory.FormatValue("hello"));
        Assert.Equal(timestamp.ToString("O"), PushdownPredicateFactory.FormatValue(timestamp));
    }

    [Fact]
    public void PushdownPredicateFactory_builds_comparison_and_startswith_predicates()
    {
        var comparison = PushdownPredicateFactory.CreateComparison<NullableScoreRow>(
            (Expression<Func<NullableScoreRow, int?>>)(row => row.Score),
            ComparisonOperator.GreaterThanOrEqual,
            5);
        var startsWith = PushdownPredicateFactory.CreateStartsWith<TestRow>(row => row.Name, "al");

        Assert.Equal("Score", comparison.MemberPath);
        Assert.Equal("Score >= 5", comparison.Description);
        Assert.True(comparison.RowPredicate(new NullableScoreRow { Score = 5 }));
        Assert.False(comparison.RowPredicate(new NullableScoreRow { Score = 4 }));

        Assert.Equal("Name", startsWith.MemberPath);
        Assert.Equal("Name.StartsWith(\"al\")", startsWith.Description);
        Assert.True(startsWith.RowPredicate(new TestRow { Name = "alpha" }));
        Assert.False(startsWith.RowPredicate(new TestRow { Name = "beta" }));
    }

    [Fact]
    public void PartitionPruner_returns_empty_for_non_partitioned_paths()
    {
        var predicate = PushdownPredicateFactory.CreateComparison<TestRow>(
            (Expression<Func<TestRow, string>>)(row => row.Country),
            ComparisonOperator.Equal,
            "DE");

        var decisions = PartitionPruner.Evaluate(@"C:\data\file.parquet", new[] { predicate });

        Assert.Empty(decisions);
    }

    [Fact]
    public void PartitionPruner_matches_partition_values_for_direct_and_nested_paths()
    {
        var directPredicate = PushdownPredicateFactory.CreateComparison<TestRow>(
            (Expression<Func<TestRow, string>>)(row => row.Country),
            ComparisonOperator.Equal,
            "DE");
        var nestedPredicate = PushdownPredicateFactory.CreateComparison<NestedTestRow>(
            (Expression<Func<NestedTestRow, string>>)(row => row.Address.City),
            ComparisonOperator.Equal,
            "Berlin");

        var directDecision = Assert.Single(PartitionPruner.Evaluate(
            @"C:\lake\Country=DE\part-0001.parquet",
            new[] { directPredicate }));
        var nestedDecision = Assert.Single(PartitionPruner.Evaluate(
            @"C:\lake\year=2024\City=Berlin\part-0002.parquet",
            new[] { nestedPredicate }));

        Assert.True(directDecision.MayMatch);
        Assert.Equal("partition", directDecision.Source);
        Assert.Contains("before opening the file", directDecision.Reason, StringComparison.Ordinal);

        Assert.True(nestedDecision.MayMatch);
        Assert.Equal("Address.City == \"Berlin\"", nestedDecision.Predicate);
    }

    [Fact]
    public void PartitionPruner_matches_partition_values_for_unix_style_paths()
    {
        var predicate = PushdownPredicateFactory.CreateComparison<TestRow>(
            (Expression<Func<TestRow, string>>)(row => row.Country),
            ComparisonOperator.Equal,
            "DE");

        var decision = Assert.Single(PartitionPruner.Evaluate(
            "/lake/year=2024/Country=DE/part-0001.parquet",
            new[] { predicate }));

        Assert.True(decision.MayMatch);
        Assert.Equal("Country == \"DE\"", decision.Predicate);
    }

    [Fact]
    public void PartitionPruner_handles_prefix_checks_and_unparseable_values()
    {
        var startsWithPredicate = PushdownPredicateFactory.CreateStartsWith<TestRow>(row => row.Name, "al");
        var numericPredicate = PushdownPredicateFactory.CreateComparison<TestRow>(
            (Expression<Func<TestRow, int>>)(row => row.Age),
            ComparisonOperator.GreaterThan,
            18);

        var startsWithDecision = Assert.Single(PartitionPruner.Evaluate(
            @"C:\lake\Name=beta\part-0001.parquet",
            new[] { startsWithPredicate }));
        var invalidNumericDecision = Assert.Single(PartitionPruner.Evaluate(
            @"C:\lake\Age=oops\part-0001.parquet",
            new[] { numericPredicate }));

        Assert.False(startsWithDecision.MayMatch);
        Assert.Contains("prefix", startsWithDecision.Reason, StringComparison.Ordinal);

        Assert.True(invalidNumericDecision.MayMatch);
        Assert.Contains("could not be interpreted", invalidNumericDecision.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public void ParquetOptionsFactory_clone_copies_mutable_state()
    {
        var source = new ParquetOptions
        {
            TreatByteArrayAsString = true,
            UseDictionaryEncoding = true,
            FooterEncryptionKey = "footer-key",
            FooterEncryptionKeyMetadata = new byte[] { 1, 2, 3 },
            FooterSigningKey = "signing-key",
            FooterSigningKeyMetadata = new byte[] { 4, 5, 6 },
            AADPrefix = "aad",
            SupplyAadPrefix = true,
            UseCtrVariant = true,
            ColumnKeyResolver = (path, metadata) => path.Count > 0 && metadata is not null ? "column-key" : null
        };
        source.BloomFilterOptionsByColumn["Name"] = new ParquetOptions.BloomFilterOptions
        {
            EnableBloomFilters = true,
            BloomFilterFpp = 0.05f,
            BloomFilterBitsPerValueOverride = 12
        };
        source.ColumnKeys["Name"] = new ParquetOptions.ColumnKeySpec("col-key", new byte[] { 7, 8, 9 });

        var clone = ParquetOptionsFactory.Clone(source);

        Assert.NotSame(source, clone);
        Assert.True(clone.TreatByteArrayAsString);
        Assert.True(clone.UseDictionaryEncoding);
        Assert.Equal(source.FooterEncryptionKey, clone.FooterEncryptionKey);
        Assert.Equal(source.FooterEncryptionKeyMetadata, clone.FooterEncryptionKeyMetadata);
        Assert.NotSame(source.FooterEncryptionKeyMetadata, clone.FooterEncryptionKeyMetadata);
        Assert.Equal(source.FooterSigningKeyMetadata, clone.FooterSigningKeyMetadata);
        Assert.NotSame(source.FooterSigningKeyMetadata, clone.FooterSigningKeyMetadata);
        Assert.True(clone.BloomFilterOptionsByColumn["Name"].EnableBloomFilters);
        Assert.Equal(0.05f, clone.BloomFilterOptionsByColumn["Name"].BloomFilterFpp);
        Assert.Equal(12, clone.BloomFilterOptionsByColumn["Name"].BloomFilterBitsPerValueOverride);
        Assert.Equal("col-key", clone.ColumnKeys["Name"].Key);
        Assert.Equal(new byte[] { 7, 8, 9 }, clone.ColumnKeys["Name"].KeyMetadata);
        Assert.NotSame(source.ColumnKeys["Name"].KeyMetadata, clone.ColumnKeys["Name"].KeyMetadata);
        Assert.Same(source.ColumnKeyResolver, clone.ColumnKeyResolver);
    }

    [Fact]
    public void SourceMaterializationPlanBuilder_can_create_filter_only_plan()
    {
        var schema = ParquetWritePlanBuilder.BuildSchema<TestRow>();

        var plan = SourceMaterializationPlanBuilder.Build<TestRow>(
            schema,
            PushdownFilter<TestRow>.Empty,
            new Expression<Func<TestRow, bool>>[] { row => row.Age >= 18 },
            projection: null,
            includeDefaultResultPaths: false);

        Assert.False(plan.RequiresFullMaterialization);
        Assert.Equal(new[] { "Age" }, plan.FilterColumnPaths);
        Assert.Empty(plan.ResultColumnPaths);
        Assert.Equal(new[] { "Age" }, plan.RequiredColumnPaths);
    }

    [Fact]
    public void SourceMaterializationPlanBuilder_expands_nested_bindings_and_assigns_values()
    {
        var schema = ParquetWritePlanBuilder.BuildSchema<NestedTestRow>();

        var plan = SourceMaterializationPlanBuilder.Build<NestedTestRow, TestAddress>(
            schema,
            PushdownFilter<NestedTestRow>.Empty,
            Array.Empty<Expression<Func<NestedTestRow, bool>>>(),
            (Expression<Func<NestedTestRow, TestAddress>>)(row => row.Address));

        var cityBinding = plan.FindBinding("Address.City");
        var postalCodeBinding = plan.FindBinding("Address.PostalCode");
        var row = new NestedTestRow { Address = null! };

        Assert.False(plan.RequiresFullMaterialization);
        Assert.NotNull(cityBinding);
        Assert.NotNull(postalCodeBinding);

        cityBinding!.Assign(row, "Berlin");
        postalCodeBinding!.Assign(row, "10115");

        Assert.Equal("Berlin", row.Address.City);
        Assert.Equal("10115", row.Address.PostalCode);
        Assert.Equal("Berlin", cityBinding.Read(row));
    }

    [Fact]
    public void SourceMaterializationPlanBuilder_converts_collection_values_for_list_bindings()
    {
        var schema = ParquetWritePlanBuilder.BuildSchema<ListCollectionRow>();

        var plan = SourceMaterializationPlanBuilder.Build<ListCollectionRow, List<string>>(
            schema,
            PushdownFilter<ListCollectionRow>.Empty,
            Array.Empty<Expression<Func<ListCollectionRow, bool>>>(),
            (Expression<Func<ListCollectionRow, List<string>>>)(row => row.Tags));

        var binding = plan.FindBinding("Tags");
        var row = new ListCollectionRow();

        Assert.NotNull(binding);
        binding!.Assign(row, new[] { "red", "blue" });

        Assert.Equal(new[] { "red", "blue" }, row.Tags);
        Assert.Equal(new[] { "red", "blue" }, Assert.IsAssignableFrom<IEnumerable<string>>(binding.Read(row)!));
    }

    [Fact]
    public void SourceMaterializationPlanBuilder_returns_full_when_schema_does_not_match_default_paths()
    {
        var schema = ParquetWritePlanBuilder.BuildSchema<TestRow>();

        var plan = SourceMaterializationPlanBuilder.Build<SchemaMismatchRow>(
            schema,
            PushdownFilter<SchemaMismatchRow>.Empty,
            Array.Empty<Expression<Func<SchemaMismatchRow, bool>>>(),
            projection: null,
            includeDefaultResultPaths: true);

        Assert.True(plan.RequiresFullMaterialization);
    }

    private sealed record CacheEntry(long AccessTicks);

    private enum SampleStatus
    {
        New = 1,
        Done = 2
    }
}
