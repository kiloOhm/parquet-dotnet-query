namespace Parquet.Query.Viewer.WinUI;

public partial class App : MauiWinUIApplication
{
    public App()
    {
        InitializeComponent();
        UnhandledException += OnUnhandledException;
    }

    private static void OnUnhandledException(object sender, Microsoft.UI.Xaml.UnhandledExceptionEventArgs e)
    {
        Parquet.Query.Viewer.CrashLog.Write(e.Exception);
        e.Handled = false; // let the app terminate after logging
    }

    protected override MauiApp CreateMauiApp() => MauiProgram.CreateMauiApp();
}
