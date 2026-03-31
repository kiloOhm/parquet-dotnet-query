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
        if (webView.Handler?.PlatformView is not Microsoft.UI.Xaml.Controls.WebView2 nativeWebView)
            return;

        _nativeWebView = nativeWebView;
        await nativeWebView.EnsureCoreWebView2Async();

        var wwwrootPath = Path.Combine(AppContext.BaseDirectory, "wwwroot");
        nativeWebView.CoreWebView2.SetVirtualHostNameToFolderMapping(
            "parquet-viewer.local",
            wwwrootPath,
            Microsoft.Web.WebView2.Core.CoreWebView2HostResourceAccessKind.Allow);

        nativeWebView.CoreWebView2.WebMessageReceived += OnWebMessageReceived;

#if DEBUG
        // Disable cache so React builds are never stale during development
        nativeWebView.CoreWebView2.Settings.IsGeneralAutofillEnabled = false;
        await nativeWebView.CoreWebView2.Profile.ClearBrowsingDataAsync();
        nativeWebView.CoreWebView2.OpenDevToolsWindow();
#endif

        nativeWebView.Source = new Uri("https://parquet-viewer.local/index.html");
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
#else
    private void OnHandlerChanged(object? sender, EventArgs e)
    {
        // Non-Windows platforms not yet supported
    }
#endif
}
