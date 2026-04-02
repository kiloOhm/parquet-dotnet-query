using Parquet.Query.Viewer.Services;

namespace Parquet.Query.Viewer;

public partial class MainPage : ContentPage
{
    private readonly WebViewBridge _bridge;

    public MainPage(WebViewBridge bridge)
    {
        InitializeComponent();
        _bridge = bridge;
        webView.HandlerChanged += OnHandlerChanged;
    }

#if WINDOWS
    private Microsoft.UI.Xaml.Controls.WebView2? _nativeWebView;

    private async void OnHandlerChanged(object? sender, EventArgs e)
    {
        try
        {
            if (webView.Handler?.PlatformView is not Microsoft.UI.Xaml.Controls.WebView2 nativeWebView)
                return;

            CrashLog.Write("WebView2 handler obtained, initializing CoreWebView2...");
            _nativeWebView = nativeWebView;
            await nativeWebView.EnsureCoreWebView2Async();
            CrashLog.Write("CoreWebView2 ready");

            var wwwrootPath = Path.Combine(AppContext.BaseDirectory, "wwwroot");
            CrashLog.Write($"Mapping virtual host to: {wwwrootPath} (exists={System.IO.Directory.Exists(wwwrootPath)})");
            nativeWebView.CoreWebView2.SetVirtualHostNameToFolderMapping(
                "parquet-viewer.local",
                wwwrootPath,
                Microsoft.Web.WebView2.Core.CoreWebView2HostResourceAccessKind.Allow);

            nativeWebView.CoreWebView2.WebMessageReceived += OnWebMessageReceived;

            // Open the file passed via command-line ("Open with") once React is ready
            nativeWebView.CoreWebView2.NavigationCompleted += OnNavigationCompleted;

            // Enable native file drag-and-drop onto the WebView
            nativeWebView.AllowDrop = true;
            nativeWebView.DragOver += OnDragOver;
            nativeWebView.Drop += OnDrop;

#if DEBUG
            // Disable cache so React builds are never stale during development
            nativeWebView.CoreWebView2.Settings.IsGeneralAutofillEnabled = false;
            await nativeWebView.CoreWebView2.Profile.ClearBrowsingDataAsync();
            nativeWebView.CoreWebView2.OpenDevToolsWindow();
#endif

            nativeWebView.Source = new Uri("https://parquet-viewer.local/index.html");
            CrashLog.Write("Navigation started");
        }
        catch (Exception ex)
        {
            CrashLog.Write(ex);
        }
    }

    private void OnNavigationCompleted(
        Microsoft.Web.WebView2.Core.CoreWebView2 sender,
        Microsoft.Web.WebView2.Core.CoreWebView2NavigationCompletedEventArgs args)
    {
        // Unsubscribe — we only need the first navigation (app load)
        sender.NavigationCompleted -= OnNavigationCompleted;

        if (!args.IsSuccess || _nativeWebView is null)
            return;

        var startupFile = App.StartupFilePath;
        if (startupFile is null)
            return;

        System.Diagnostics.Debug.WriteLine($"[MainPage] Opening startup file: {startupFile}");

        _ = Task.Run(async () =>
        {
            var pushJson = await _bridge.HandleFileDropAsync(startupFile);
            MainThread.BeginInvokeOnMainThread(() =>
            {
                _nativeWebView.CoreWebView2.PostWebMessageAsJson(pushJson);
            });
        });
    }

    private void OnWebMessageReceived(
        Microsoft.Web.WebView2.Core.CoreWebView2 sender,
        Microsoft.Web.WebView2.Core.CoreWebView2WebMessageReceivedEventArgs args)
    {
        var json = args.WebMessageAsJson;

        _ = Task.Run(async () =>
        {
            var response = await _bridge.HandleMessageAsync(json);
            MainThread.BeginInvokeOnMainThread(() =>
            {
                sender.PostWebMessageAsJson(response);
            });
        });
    }

    private static readonly string[] s_parquetExtensions = [".parquet", ".par"];

    private void OnDragOver(object sender, Microsoft.UI.Xaml.DragEventArgs e)
    {
        if (e.DataView.Contains(Windows.ApplicationModel.DataTransfer.StandardDataFormats.StorageItems))
        {
            e.AcceptedOperation = Windows.ApplicationModel.DataTransfer.DataPackageOperation.Copy;
            e.DragUIOverride.Caption = "Open in Parquet Viewer";
            e.DragUIOverride.IsCaptionVisible = true;
            e.DragUIOverride.IsGlyphVisible = true;
        }
    }

    private async void OnDrop(object sender, Microsoft.UI.Xaml.DragEventArgs e)
    {
        if (!e.DataView.Contains(Windows.ApplicationModel.DataTransfer.StandardDataFormats.StorageItems))
            return;

        var items = await e.DataView.GetStorageItemsAsync();
        var file = items
            .OfType<Windows.Storage.StorageFile>()
            .FirstOrDefault(f => s_parquetExtensions.Contains(f.FileType, StringComparer.OrdinalIgnoreCase));

        if (file is null || _nativeWebView is null)
            return;

        var filePath = file.Path;
        System.Diagnostics.Debug.WriteLine($"[MainPage] File dropped: {filePath}");

        _ = Task.Run(async () =>
        {
            var pushJson = await _bridge.HandleFileDropAsync(filePath);
            MainThread.BeginInvokeOnMainThread(() =>
            {
                _nativeWebView.CoreWebView2.PostWebMessageAsJson(pushJson);
            });
        });
    }
#else
    private void OnHandlerChanged(object? sender, EventArgs e)
    {
        // Non-Windows platforms not yet supported
    }
#endif
}
