using System.Collections.ObjectModel;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using Parquet.Query.Expressions;
using Parquet.Query.Internal;
using Parquet.Query.Planning;
using Parquet.Query.Pushdown;
using Parquet.Serialization;

namespace Parquet.Query;

public static class ParquetQuery
{
    public static ParquetQuery<T, T> FromFile<T>(string filePath, ParquetOptions? parquetOptions = null)
        where T : class, new()
        => ParquetQuery<T, T>.FromFile(filePath, parquetOptions);
}

public sealed class ParquetQuery<TSource, TResult>
    where TSource : class, new()
{
    private readonly string _filePath;
    private readonly ParquetOptions? _parquetOptions;
    private readonly PushdownFilter<TSource> _pushdownFilter;
    private readonly IReadOnlyList<Expression<Func<TSource, bool>>> _wherePredicates;
    private readonly IReadOnlyList<string> _residualPredicates;
    private readonly Expression<Func<TSource, TResult>>? _projection;
    private readonly bool _strictPushdown;

    private ParquetQuery(
        string filePath,
        ParquetOptions? parquetOptions,
        PushdownFilter<TSource> pushdownFilter,
        IReadOnlyList<Expression<Func<TSource, bool>>> wherePredicates,
        IReadOnlyList<string> residualPredicates,
        Expression<Func<TSource, TResult>>? projection,
        bool strictPushdown)
    {
        _filePath = filePath;
        _parquetOptions = parquetOptions;
        _pushdownFilter = pushdownFilter;
        _wherePredicates = wherePredicates;
        _residualPredicates = residualPredicates;
        _projection = projection;
        _strictPushdown = strictPushdown;
    }

    public static ParquetQuery<TSource, TResult> FromFile(string filePath, ParquetOptions? parquetOptions = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        return new ParquetQuery<TSource, TResult>(
            filePath,
            parquetOptions,
            PushdownFilter<TSource>.Empty,
            Array.Empty<Expression<Func<TSource, bool>>>(),
            Array.Empty<string>(),
            projection: null,
            strictPushdown: false);
    }

    public ParquetQuery<TSource, TResult> Pushdown(PushdownFilter<TSource> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);

        return new ParquetQuery<TSource, TResult>(
            _filePath,
            _parquetOptions,
            _pushdownFilter.And(filter),
            _wherePredicates,
            _residualPredicates,
            _projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TResult> Pushdown(Func<PushdownFilterBuilder<TSource>, PushdownFilterBuilder<TSource>> configure) =>
        Pushdown(global::Parquet.Query.Pushdown.Pushdown.For(configure));

    public ParquetQuery<TSource, TResult> Where(Expression<Func<TSource, bool>> predicate)
    {
        ArgumentNullException.ThrowIfNull(predicate);

        var split = PredicatePushdownExtractor.Extract(predicate);

        return new ParquetQuery<TSource, TResult>(
            _filePath,
            _parquetOptions,
            _pushdownFilter.And(split.PushdownFilter),
            new ReadOnlyCollection<Expression<Func<TSource, bool>>>(_wherePredicates.Concat(new[] { predicate }).ToArray()),
            new ReadOnlyCollection<string>(_residualPredicates.Concat(split.UnsupportedExpressions).ToArray()),
            _projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TNextResult> Select<TNextResult>(Expression<Func<TSource, TNextResult>> projection)
    {
        ArgumentNullException.ThrowIfNull(projection);

        return new ParquetQuery<TSource, TNextResult>(
            _filePath,
            _parquetOptions,
            _pushdownFilter,
            _wherePredicates,
            _residualPredicates,
            projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TResult> StrictPushdown(bool enabled = true) =>
        new(
            _filePath,
            _parquetOptions,
            _pushdownFilter,
            _wherePredicates,
            _residualPredicates,
            _projection,
            enabled);

    public ParquetQuery<TSource, TResult> WithParquetOptions(ParquetOptions parquetOptions)
    {
        ArgumentNullException.ThrowIfNull(parquetOptions);

        return new ParquetQuery<TSource, TResult>(
            _filePath,
            ParquetOptionsFactory.Clone(parquetOptions),
            _pushdownFilter,
            _wherePredicates,
            _residualPredicates,
            _projection,
            _strictPushdown);
    }

    public ParquetQuery<TSource, TResult> ConfigureParquetOptions(Action<ParquetOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = ParquetOptionsFactory.Clone(_parquetOptions);
        configure(options);
        return WithParquetOptions(options);
    }

    public ParquetQuery<TSource, TResult> WithFooterKey(string footerEncryptionKey, byte[]? keyMetadata = null) =>
        ConfigureParquetOptions(options =>
        {
            options.FooterEncryptionKey = footerEncryptionKey;
            options.FooterEncryptionKeyMetadata = keyMetadata?.ToArray();
        });

    public ParquetQuery<TSource, TResult> WithFooterSigningKey(string footerSigningKey, byte[]? keyMetadata = null) =>
        ConfigureParquetOptions(options =>
        {
            options.FooterSigningKey = footerSigningKey;
            options.FooterSigningKeyMetadata = keyMetadata?.ToArray();
        });

    public ParquetQuery<TSource, TResult> UsePlaintextFooter(bool enabled = true) =>
        ConfigureParquetOptions(options => options.UsePlaintextFooter = enabled);

    public ParquetQuery<TSource, TResult> WithAadPrefix(string aadPrefix, bool supplyOutOfBand = false) =>
        ConfigureParquetOptions(options =>
        {
            options.AADPrefix = aadPrefix;
            options.SupplyAadPrefix = supplyOutOfBand;
        });

    public ParquetQuery<TSource, TResult> UseCtrVariant(bool enabled = true) =>
        ConfigureParquetOptions(options => options.UseCtrVariant = enabled);

    public ParquetQuery<TSource, TResult> WithColumnKeyResolver(Func<IReadOnlyList<string>, byte[]?, string?> resolver) =>
        ConfigureParquetOptions(options => options.ColumnKeyResolver = resolver);

    public async Task<ParquetQueryPlan> PlanAsync(CancellationToken cancellationToken = default)
    {
        EnsureStrictPushdown();

        await using var stream = System.IO.File.OpenRead(_filePath);
        using var reader = await ParquetReader.CreateAsync(stream, _parquetOptions, leaveStreamOpen: false, cancellationToken);
        var materializationPlan = CreateMaterializationPlan(reader.Schema);
        return RowGroupPlanner.Build(_filePath, reader, _pushdownFilter, _residualPredicates, materializationPlan);
    }

    public async Task<string> ExplainAsync(CancellationToken cancellationToken = default)
    {
        var plan = await PlanAsync(cancellationToken);
        var builder = new StringBuilder();

        builder.AppendLine($"File: {plan.FilePath}");
        builder.AppendLine(plan.PushdownPredicates.Count == 0
            ? "Pushdown: none"
            : $"Pushdown: {string.Join(", ", plan.PushdownPredicates)}");
        builder.AppendLine(plan.ResidualPredicates.Count == 0
            ? "Residual: none"
            : $"Residual: {string.Join(", ", plan.ResidualPredicates)}");
        builder.AppendLine(plan.RequiresFullMaterialization
            ? "Read Columns: all"
            : $"Read Columns: {string.Join(", ", plan.ReadColumns)}");
        builder.AppendLine($"Row groups selected: {plan.SelectedRowGroupCount}/{plan.RowGroups.Count}");

        foreach (var rowGroup in plan.RowGroups)
        {
            builder.AppendLine($"  RG {rowGroup.Index}: {(rowGroup.ShouldRead ? "read" : "skip")} ({rowGroup.RowCount} rows)");
            foreach (var decision in rowGroup.Decisions)
            {
                builder.AppendLine($"    {decision.Predicate}: {(decision.MayMatch ? "may match" : "ruled out")} via {decision.Source} ({decision.Reason})");
            }
        }

        return builder.ToString().TrimEnd();
    }

    public async Task<IReadOnlyList<TResult>> ToListAsync(CancellationToken cancellationToken = default)
    {
        EnsureStrictPushdown();

        var rowFilter = BuildRowFilter();
        var projector = BuildProjector();
        var results = new List<TResult>();

        await using var stream = System.IO.File.OpenRead(_filePath);
        using var reader = await ParquetReader.CreateAsync(stream, _parquetOptions, leaveStreamOpen: false, cancellationToken);
        var materializationPlan = CreateMaterializationPlan(reader.Schema);
        var serializerOptions = CreateSerializerOptions();
        var plan = RowGroupPlanner.Build(_filePath, reader, _pushdownFilter, _residualPredicates, materializationPlan);

        foreach (var rowGroup in plan.RowGroups.Where(rowGroup => rowGroup.ShouldRead))
        {
            var batch = await PartialRowMaterializer<TSource>.ReadRowGroupAsync(
                _filePath,
                reader,
                rowGroup.Index,
                materializationPlan,
                serializerOptions,
                cancellationToken);

            foreach (var row in batch)
            {
                if (rowFilter(row))
                {
                    results.Add(projector(row));
                }
            }
        }

        return results;
    }

    public async IAsyncEnumerable<TResult> ToAsyncEnumerable([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        EnsureStrictPushdown();

        var rowFilter = BuildRowFilter();
        var projector = BuildProjector();

        await using var stream = System.IO.File.OpenRead(_filePath);
        using var reader = await ParquetReader.CreateAsync(stream, _parquetOptions, leaveStreamOpen: false, cancellationToken);
        var materializationPlan = CreateMaterializationPlan(reader.Schema);
        var serializerOptions = CreateSerializerOptions();
        var plan = RowGroupPlanner.Build(_filePath, reader, _pushdownFilter, _residualPredicates, materializationPlan);

        foreach (var rowGroup in plan.RowGroups.Where(rowGroup => rowGroup.ShouldRead))
        {
            var batch = await PartialRowMaterializer<TSource>.ReadRowGroupAsync(
                _filePath,
                reader,
                rowGroup.Index,
                materializationPlan,
                serializerOptions,
                cancellationToken);

            foreach (var row in batch)
            {
                if (rowFilter(row))
                {
                    yield return projector(row);
                }
            }
        }
    }

    private SourceMaterializationPlan<TSource> CreateMaterializationPlan(Parquet.Schema.ParquetSchema schema) =>
        SourceMaterializationPlanBuilder.Build<TSource, TResult>(
            schema,
            _pushdownFilter,
            _wherePredicates,
            _projection);

    private Func<TSource, bool> BuildRowFilter()
    {
        var whereDelegates = _wherePredicates.Select(predicate => predicate.Compile()).ToArray();
        if (_pushdownFilter.IsEmpty && whereDelegates.Length == 0)
        {
            return static _ => true;
        }

        return row => _pushdownFilter.Matches(row) && whereDelegates.All(predicate => predicate(row));
    }

    private Func<TSource, TResult> BuildProjector()
    {
        if (_projection is not null)
        {
            return _projection.Compile();
        }

        return static row => (TResult)(object)row;
    }

    private ParquetSerializerOptions? CreateSerializerOptions() =>
        _parquetOptions is null
            ? null
            : new ParquetSerializerOptions
            {
                ParquetOptions = _parquetOptions
            };

    private void EnsureStrictPushdown()
    {
        if (_strictPushdown && _residualPredicates.Count > 0)
        {
            throw new InvalidOperationException(
                "Strict pushdown was enabled, but the following predicates could not be translated: " +
                string.Join(", ", _residualPredicates));
        }
    }
}
