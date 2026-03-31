using System.Text.Json;
using Parquet.Query.Viewer.Models;

namespace Parquet.Query.Viewer.Services;

/// <summary>
/// Persists encryption configurations per file path so previously-opened
/// encrypted files can be reopened without re-entering keys.
/// </summary>
public sealed class EncryptionStore
{
    private static readonly JsonSerializerOptions s_json = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
    };

    private readonly string _storePath;
    private Dictionary<string, EncryptionConfig> _entries;

    public EncryptionStore()
    {
        var appData = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "ParquetViewer");
        Directory.CreateDirectory(appData);
        _storePath = Path.Combine(appData, "encryption.json");
        _entries = Load();
    }

    /// <summary>
    /// Returns the saved encryption config for a file path, or null if none exists.
    /// </summary>
    public EncryptionConfig? Get(string filePath)
    {
        var key = Normalize(filePath);
        return _entries.TryGetValue(key, out var config) ? config : null;
    }

    /// <summary>
    /// Saves (or removes) the encryption config for a file path.
    /// </summary>
    public void Set(string filePath, EncryptionConfig? config)
    {
        var key = Normalize(filePath);
        if (config is null)
            _entries.Remove(key);
        else
            _entries[key] = config;
        Save();
    }

    private static string Normalize(string filePath) =>
        Path.GetFullPath(filePath).ToUpperInvariant();

    private Dictionary<string, EncryptionConfig> Load()
    {
        try
        {
            if (System.IO.File.Exists(_storePath))
            {
                var json = System.IO.File.ReadAllText(_storePath);
                return JsonSerializer.Deserialize<Dictionary<string, EncryptionConfig>>(json, s_json)
                       ?? new();
            }
        }
        catch
        {
            // Corrupted store — start fresh
        }
        return new();
    }

    private void Save()
    {
        try
        {
            System.IO.File.WriteAllText(_storePath, JsonSerializer.Serialize(_entries, s_json));
        }
        catch (Exception ex)
        {
            System.Diagnostics.Debug.WriteLine($"[EncryptionStore] Save failed: {ex.Message}");
        }
    }
}
