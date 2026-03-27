namespace Parquet.Query.Internal;

internal sealed class SourceAccessAnalysis
{
    public SourceAccessAnalysis(IReadOnlyCollection<string> memberPaths, bool requiresFullMaterialization)
    {
        MemberPaths = memberPaths;
        RequiresFullMaterialization = requiresFullMaterialization;
    }

    public IReadOnlyCollection<string> MemberPaths { get; }

    public bool RequiresFullMaterialization { get; }
}
