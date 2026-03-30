namespace Parquet.Query.Viewer;

public partial class App : Application
{
    private readonly IServiceProvider _services;

    public App(IServiceProvider services)
    {
        InitializeComponent();
        _services = services;
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
