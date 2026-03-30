namespace Parquet.Query.Compatibility;

internal static class PlatformCompatibility
{
    public static bool IsWindows()
    {
#if NET6_0_OR_GREATER
        return OperatingSystem.IsWindows();
#else
        switch (Environment.OSVersion.Platform)
        {
            case PlatformID.Win32NT:
            case PlatformID.Win32S:
            case PlatformID.Win32Windows:
            case PlatformID.WinCE:
                return true;
            default:
                return false;
        }
#endif
    }
}
