using System.Text.Json;

namespace Parquet.Query.Viewer.Models;

public sealed record BridgeRequest(string Id, string Method, JsonElement? Params);
public sealed record BridgeResponse(string Id, object? Result = null, string? Error = null);
