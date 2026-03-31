namespace Parquet.Query.Viewer.Models;

public sealed record QueryPredicate(
    string Column,
    string Operator,
    string Value,
    string? Value2 = null,
    int? MaxEdits = null,
    int? PrefixLength = null,
    bool? Transpositions = null);

public sealed record QueryRequest(
    QueryPredicate[] Predicates,
    int Offset = 0,
    int Limit = 200);

public sealed record QueryPlan(
    int TotalRowGroups,
    int SelectedRowGroups,
    int SkippedRowGroups,
    RowGroupDecision[] Decisions,
    long TotalRows,
    long CandidateRows,
    long MatchedRows,
    double ExecutionMs);

public sealed record RowGroupDecision(
    int Index,
    bool ShouldRead,
    string Reason,
    long RowCount);

public sealed record QueryResult(
    QueryPlan Plan,
    DataPage Data);
