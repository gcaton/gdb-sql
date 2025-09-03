using System.Threading.Channels;

namespace GdbToSql;

public class LayerBatch
{
    public string LayerName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public List<Dictionary<string, object?>> Features { get; set; } = new();
    public bool IsLastBatch { get; set; }
    public bool IsFirstBatch { get; set; }
}

public class LayerInfo
{
    public string LayerName { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public long TotalFeatures { get; set; }
    public int LayerIndex { get; set; }
}

public class StreamingProgress
{
    private readonly object _lock = new();
    private readonly Dictionary<string, (long Processed, long Total)> _layerProgress = new();
    
    public void UpdateProgress(string layerName, long processed, long total)
    {
        lock (_lock)
        {
            _layerProgress[layerName] = (processed, total);
        }
    }
    
    public void ShowProgress()
    {
        lock (_lock)
        {
            var totalProcessed = _layerProgress.Values.Sum(p => p.Processed);
            var totalFeatures = _layerProgress.Values.Sum(p => p.Total);
            var completedLayers = _layerProgress.Count(p => p.Value.Processed == p.Value.Total);
            
            Console.WriteLine($"Progress: {completedLayers}/{_layerProgress.Count} layers complete, {totalProcessed}/{totalFeatures} features processed");
        }
    }
}