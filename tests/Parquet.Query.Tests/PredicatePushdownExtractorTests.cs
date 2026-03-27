using System.Linq.Expressions;
using Parquet.Query.Expressions;
using Parquet.Query.Pushdown;

namespace Parquet.Query.Tests;

public sealed class PredicatePushdownExtractorTests
{
    [Fact]
    public void Extract_splits_supported_and_residual_predicates()
    {
        Expression<Func<TestRow, bool>> predicate =
            row => row.Country == "DE" && row.Age >= 18 && IsEligible(row);

        var split = PredicatePushdownExtractor.Extract(predicate);

        Assert.Equal(2, split.PushdownFilter.Predicates.Count);
        Assert.Single(split.UnsupportedExpressions);
        Assert.NotNull(split.ResidualPredicate);
        Assert.Contains("IsEligible", split.UnsupportedExpressions[0]);
    }

    [Fact]
    public void Extract_handles_reversed_binary_comparisons()
    {
        Expression<Func<TestRow, bool>> predicate = row => 18 <= row.Age;

        var split = PredicatePushdownExtractor.Extract(predicate);

        var comparison = Assert.IsType<ComparisonPushdownPredicate<TestRow>>(Assert.Single(split.PushdownFilter.Predicates));
        Assert.Equal(ComparisonOperator.GreaterThanOrEqual, comparison.Operator);
        Assert.Equal(18, comparison.Value);
    }

    [Fact]
    public void Extract_supports_startswith_with_captured_prefix()
    {
        const string prefix = "br";
        Expression<Func<TestRow, bool>> predicate =
            row => row.Name.StartsWith(prefix, StringComparison.Ordinal);

        var split = PredicatePushdownExtractor.Extract(predicate);

        var startsWith = Assert.IsType<StartsWithPushdownPredicate<TestRow>>(Assert.Single(split.PushdownFilter.Predicates));
        Assert.Equal("Name", startsWith.MemberPath);
        Assert.Equal("br", startsWith.Prefix);
        Assert.Null(split.ResidualPredicate);
    }

    [Fact]
    public void Extract_uses_closed_over_member_values()
    {
        var criteria = new TestCriteria { Country = "DE" };
        Expression<Func<TestRow, bool>> predicate = row => row.Country == criteria.Country;

        var split = PredicatePushdownExtractor.Extract(predicate);

        var comparison = Assert.IsType<ComparisonPushdownPredicate<TestRow>>(Assert.Single(split.PushdownFilter.Predicates));
        Assert.Equal(ComparisonOperator.Equal, comparison.Operator);
        Assert.Equal("DE", comparison.Value);
    }

    [Fact]
    public void Extract_keeps_non_ordinal_startswith_as_residual()
    {
        Expression<Func<TestRow, bool>> predicate =
            row => row.Name.StartsWith("br", StringComparison.InvariantCultureIgnoreCase);

        var split = PredicatePushdownExtractor.Extract(predicate);

        Assert.Empty(split.PushdownFilter.Predicates);
        Assert.Single(split.UnsupportedExpressions);
        Assert.Contains("InvariantCultureIgnoreCase", split.UnsupportedExpressions[0]);
        Assert.NotNull(split.ResidualPredicate);
    }

    [Fact]
    public void Extract_keeps_or_expressions_as_residual()
    {
        Expression<Func<TestRow, bool>> predicate =
            row => row.Country == "DE" || row.Age >= 18;

        var split = PredicatePushdownExtractor.Extract(predicate);

        Assert.Empty(split.PushdownFilter.Predicates);
        Assert.Single(split.UnsupportedExpressions);
        Assert.NotNull(split.ResidualPredicate);
        Assert.True(split.ResidualPredicate!.Compile()(new TestRow { Country = "DE", Age = 1 }));
    }

    [Fact]
    public void Extract_reports_why_a_predicate_was_not_pushed_down()
    {
        Expression<Func<TestRow, bool>> orPredicate =
            row => row.Country == "DE" || row.Age >= 18;
        Expression<Func<TestRow, bool>> startsWithPredicate =
            row => row.Name.StartsWith("br", StringComparison.InvariantCultureIgnoreCase);

        var orSplit = PredicatePushdownExtractor.Extract(orPredicate);
        var startsWithSplit = PredicatePushdownExtractor.Extract(startsWithPredicate);

        Assert.Contains(orSplit.Diagnostics, diagnostic => diagnostic.Reason.Contains("Logical OR", StringComparison.Ordinal));
        Assert.Contains(startsWithSplit.Diagnostics, diagnostic => diagnostic.Reason.Contains("StringComparison.Ordinal", StringComparison.Ordinal));
    }

    private static bool IsEligible(TestRow row) => row.Country.StartsWith("D", StringComparison.Ordinal) && row.Age % 2 == 0;

    private sealed class TestCriteria
    {
        public string Country { get; init; } = string.Empty;
    }
}
