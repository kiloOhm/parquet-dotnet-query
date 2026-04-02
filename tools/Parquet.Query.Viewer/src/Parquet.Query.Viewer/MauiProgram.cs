using Microsoft.Extensions.Logging;
using Parquet.Query.Viewer.Services;

namespace Parquet.Query.Viewer;

public static class MauiProgram
{
    public static MauiApp CreateMauiApp()
    {
        CrashLog.Write($"Starting ParquetViewer from {AppContext.BaseDirectory}");

        try
        {
            var builder = MauiApp.CreateBuilder();
            builder
                .UseMauiApp<App>()
                .ConfigureFonts(fonts =>
                {
                    fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
                    fonts.AddFont("OpenSans-Semibold.ttf", "OpenSansSemibold");
                });

            builder.Services.AddSingleton<ParquetService>();
            builder.Services.AddSingleton<PredicateEvaluator>();
            builder.Services.AddSingleton<EncryptionStore>();
            builder.Services.AddSingleton<WebViewBridge>();
            builder.Services.AddTransient<MainPage>();

#if DEBUG
            builder.Logging.AddDebug();
#endif

            var app = builder.Build();
            CrashLog.Write("MAUI app built successfully");
            return app;
        }
        catch (Exception ex)
        {
            CrashLog.Write(ex);
            throw;
        }
    }
}
