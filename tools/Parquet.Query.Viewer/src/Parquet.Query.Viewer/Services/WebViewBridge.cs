using System.Text.Json;
using Parquet.Query.Viewer.Models;

namespace Parquet.Query.Viewer.Services;

public sealed class WebViewBridge
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        PropertyNameCaseInsensitive = true,
        WriteIndented = false,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
    };

    private readonly ParquetService _parquetService;

    public WebViewBridge(ParquetService parquetService)
    {
        _parquetService = parquetService;
    }

    public async Task<string> HandleMessageAsync(string messageJson)
    {
        BridgeRequest? request = null;
        try
        {
            var json = messageJson;

            if (json.StartsWith('"') && json.EndsWith('"'))
            {
                json = JsonSerializer.Deserialize<string>(json) ?? json;
            }

            request = JsonSerializer.Deserialize<BridgeRequest>(json, s_jsonOptions);
            if (request is null)
                return SerializeError("null", "Invalid request format.");

            var result = await DispatchAsync(request);
            return SerializeResponse(request.Id, result);
        }
        catch (Exception ex)
        {
            return SerializeError(request?.Id ?? "unknown", ex.Message);
        }
    }

    private async Task<object?> DispatchAsync(BridgeRequest request)
    {
        return request.Method switch
        {
            "pickFile" => await HandlePickFileAsync(),
            "openFile" => await HandleOpenFileAsync(request.Params),
            "getSchema" => HandleGetSchema(),
            "getMetadata" => HandleGetMetadata(),
            "getData" => await HandleGetDataAsync(request.Params),
            "executeQuery" => await HandleExecuteQueryAsync(request.Params),
            "getQueryPlan" => HandleGetQueryPlan(request.Params),
            _ => throw new InvalidOperationException($"Unknown method: {request.Method}")
        };
    }

    private async Task<object?> HandlePickFileAsync()
    {
        var result = await MainThread.InvokeOnMainThreadAsync(async () =>
        {
            var pickResult = await FilePicker.Default.PickAsync(new PickOptions
            {
                PickerTitle = "Select a Parquet file",
                FileTypes = new FilePickerFileType(new Dictionary<DevicePlatform, IEnumerable<string>>
                {
                    { DevicePlatform.WinUI, new[] { ".parquet", ".par" } }
                })
            });
            return pickResult?.FullPath;
        });

        if (result is null) return new { cancelled = true };

        var fileInfo = await _parquetService.OpenFileAsync(result);
        return new { cancelled = false, file = fileInfo };
    }

    private async Task<object?> HandleOpenFileAsync(JsonElement? param)
    {
        var path = param?.GetProperty("path").GetString()
            ?? throw new ArgumentException("Missing 'path' parameter.");

        EncryptionConfig? encryption = null;
        if (param?.TryGetProperty("encryption", out var encEl) == true)
        {
            encryption = JsonSerializer.Deserialize<EncryptionConfig>(encEl.GetRawText(), s_jsonOptions);
        }

        return await _parquetService.OpenFileAsync(path, encryption);
    }

    private object? HandleGetSchema()
    {
        return _parquetService.GetSchema();
    }

    private object? HandleGetMetadata()
    {
        return _parquetService.GetMetadata();
    }

    private async Task<object?> HandleGetDataAsync(JsonElement? param)
    {
        var offset = param?.TryGetProperty("offset", out var o) == true ? o.GetInt32() : 0;
        var limit = param?.TryGetProperty("limit", out var l) == true ? l.GetInt32() : 200;
        return await _parquetService.GetDataAsync(offset, limit);
    }

    private async Task<object?> HandleExecuteQueryAsync(JsonElement? param)
    {
        var request = param.HasValue
            ? JsonSerializer.Deserialize<QueryRequest>(param.Value.GetRawText(), s_jsonOptions)
            : null;
        if (request is null)
            throw new ArgumentException("Missing query request.");

        return await _parquetService.ExecuteQueryAsync(request);
    }

    private object? HandleGetQueryPlan(JsonElement? param)
    {
        var predicates = param?.TryGetProperty("predicates", out var p) == true
            ? JsonSerializer.Deserialize<QueryPredicate[]>(p.GetRawText(), s_jsonOptions) ?? []
            : [];

        return _parquetService.GetQueryPlan(predicates);
    }

    private static string SerializeResponse(string id, object? result)
    {
        return JsonSerializer.Serialize(new BridgeResponse(id, result), s_jsonOptions);
    }

    private static string SerializeError(string id, string error)
    {
        return JsonSerializer.Serialize(new BridgeResponse(id, Error: error), s_jsonOptions);
    }
}
