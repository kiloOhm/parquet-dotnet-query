namespace Parquet.Query.Viewer;

public partial class App : Application
{
    private static readonly string[] s_parquetExtensions = [".parquet", ".par"];
    private readonly IServiceProvider _services;

    /// <summary>
    /// File path passed via command-line (e.g. Windows "Open with").
    /// Null when the app is launched without a file argument.
    /// </summary>
    internal static string? StartupFilePath { get; private set; }

    public App(IServiceProvider services)
    {
        InitializeComponent();
        _services = services;

        // argv[0] = exe path; argv[1] = file path from "Open with" / shell association
        var args = Environment.GetCommandLineArgs();
        if (args.Length > 1)
        {
            var candidate = args[1];
            if (File.Exists(candidate) &&
                s_parquetExtensions.Contains(Path.GetExtension(candidate), StringComparer.OrdinalIgnoreCase))
            {
                StartupFilePath = candidate;
            }
        }
    }

    protected override Window CreateWindow(IActivationState? activationState)
    {
        return new Window(_services.GetRequiredService<MainPage>())
        {
            Title = "Parquet Viewer",
            Width = 1400,
            Height = 900
        };
    }
}
