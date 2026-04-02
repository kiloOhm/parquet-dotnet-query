namespace Parquet.Query.Viewer;

/// <summary>
/// Writes a crash log next to the executable so users can diagnose silent failures.
/// </summary>
internal static class CrashLog
{
    private static readonly string s_logPath = Path.Combine(
        AppContext.BaseDirectory, "ParquetViewer.log");

    internal static void Write(Exception? ex)
    {
        try
        {
            var entry = $"[{DateTime.UtcNow:O}] {ex}";
            System.IO.File.AppendAllText(s_logPath, entry + Environment.NewLine + Environment.NewLine);
        }
        catch
        {
            // Nothing we can do if logging itself fails.
        }
    }

    internal static void Write(string message)
    {
        try
        {
            var entry = $"[{DateTime.UtcNow:O}] {message}";
            System.IO.File.AppendAllText(s_logPath, entry + Environment.NewLine);
        }
        catch
        {
            // Nothing we can do if logging itself fails.
        }
    }
}
