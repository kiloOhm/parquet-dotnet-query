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
    private readonly EncryptionStore _encryptionStore;

    public WebViewBridge(ParquetService parquetService, EncryptionStore encryptionStore)
    {
        _parquetService = parquetService;
        _encryptionStore = encryptionStore;
    }

    public async Task<string> HandleMessageAsync(string messageJson)
    {
        BridgeRequest? request = null;
        try
        {
            var json = messageJson;
            System.Diagnostics.Debug.WriteLine($"[Bridge] Raw message: {json}");

            if (json.StartsWith('"') && json.EndsWith('"'))
            {
                json = JsonSerializer.Deserialize<string>(json) ?? json;
            }

            request = JsonSerializer.Deserialize<BridgeRequest>(json, s_jsonOptions);
            if (request is null)
                return SerializeError("null", "Invalid request format.");

            System.Diagnostics.Debug.WriteLine($"[Bridge] Dispatching: {request.Method} (id={request.Id})");
            var result = await DispatchAsync(request);
            var response = SerializeResponse(request.Id, result);
            System.Diagnostics.Debug.WriteLine($"[Bridge] Response: {response}");
            return response;
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"[Bridge] ERROR in {request?.Method ?? "?"}: {ex}");
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
            "getIndices" => HandleGetIndices(),
            "getData" => await HandleGetDataAsync(request.Params),
            "executeQuery" => await HandleExecuteQueryAsync(request.Params),
            "getQueryPlan" => await HandleGetQueryPlanAsync(request.Params),
            _ => throw new InvalidOperationException($"Unknown method: {request.Method}")
        };
    }

    private async Task<object?> HandlePickFileAsync()
    {
        var filePath = await MainThread.InvokeOnMainThreadAsync(async () =>
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

        if (filePath is null) return new { cancelled = true };

        System.Diagnostics.Debug.WriteLine($"[Bridge] pickFile: {filePath}");

        // Check if we have saved encryption for this file
        var savedEncryption = _encryptionStore.Get(filePath);
        var isEncrypted = ParquetService.DetectEncryptedFooter(filePath);
        System.Diagnostics.Debug.WriteLine($"[Bridge] PARE detected: {isEncrypted}, saved encryption: {savedEncryption is not null}");

        // If encrypted and we have saved keys, try to open automatically
        if (savedEncryption is not null)
        {
            try
            {
                var fileInfo = await _parquetService.OpenFileAsync(filePath, savedEncryption);
                return new { cancelled = false, file = fileInfo };
            }
            catch (Exception ex)
            {
                // Saved keys didn't work — fall through to prompt
                System.Diagnostics.Debug.WriteLine($"[Bridge] Saved encryption failed: {ex.Message}");
                _encryptionStore.Set(filePath, null);
            }
        }

        if (isEncrypted)
        {
            return new { cancelled = false, path = filePath, needsEncryption = true };
        }

        // Try to open the file; if it fails (e.g. plaintext-footer encryption),
        // return the path and error so the UI can prompt for keys.
        try
        {
            var fileInfo = await _parquetService.OpenFileAsync(filePath);
            return new { cancelled = false, file = fileInfo };
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"[Bridge] Open failed: {ex.Message}");
            return new { cancelled = false, path = filePath, error = ex.Message };
        }
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

        var result = await _parquetService.OpenFileAsync(path, encryption);

        // Save encryption config on successful open so we remember it next time
        _encryptionStore.Set(path, encryption);

        return result;
    }

    private object? HandleGetSchema()
    {
        return _parquetService.GetSchema();
    }

    private object? HandleGetMetadata()
    {
        return _parquetService.GetMetadata();
    }

    private object? HandleGetIndices()
    {
        return _parquetService.GetIndices();
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

    private async Task<object?> HandleGetQueryPlanAsync(JsonElement? param)
    {
        var predicates = param?.TryGetProperty("predicates", out var p) == true
            ? JsonSerializer.Deserialize<QueryPredicate[]>(p.GetRawText(), s_jsonOptions) ?? []
            : [];

        return await _parquetService.GetQueryPlanAsync(predicates);
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
