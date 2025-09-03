using MaxRev.Gdal.Core;
using Microsoft.SqlServer.Types;
using OSGeo.OGR;
using System.Data.SqlTypes;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace GdbToSql;

public class GdbReader
{
    private static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
    
    static GdbReader()
    {
        GdalBase.ConfigureAll();
    }

    public static async Task<Dictionary<string, List<Dictionary<string, object?>>>> ReadAllGdbLayersAsync(string gdbPath)
    {
        // First, get layer information from the data source
        var layerInfos = new List<(int Index, string Name, long FeatureCount)>();
        
        using (var dataSource = Ogr.Open(gdbPath, 0))
        {
            if (dataSource == null)
            {
                throw new InvalidOperationException($"Failed to open GDB at path: {gdbPath}");
            }

            var layerCount = dataSource.GetLayerCount();
            Console.WriteLine($"Found {layerCount} layers in GDB");

            for (int layerIndex = 0; layerIndex < layerCount; layerIndex++)
            {
                var layer = dataSource.GetLayerByIndex(layerIndex);
                var layerName = layer.GetName();
                var featureCount = layer.GetFeatureCount(0);
                
                if (featureCount > 0)
                {
                    layerInfos.Add((layerIndex, layerName, featureCount));
                    Console.WriteLine($"Layer '{layerName}': {featureCount} features");
                }
            }
        }
        
        if (layerInfos.Count == 0)
        {
            return new Dictionary<string, List<Dictionary<string, object?>>>();
        }
        
        Console.WriteLine($"\nReading {layerInfos.Count} layers in parallel...");
        
        // Process layers in parallel
        var allLayersData = new Dictionary<string, List<Dictionary<string, object?>>>();
        var lockObject = new object();
        var completedLayers = 0;
        
        // Limit concurrency to avoid overwhelming the system
        var maxConcurrency = Math.Min(Environment.ProcessorCount, layerInfos.Count);
        var semaphore = new SemaphoreSlim(maxConcurrency, maxConcurrency);
        
        var tasks = layerInfos.Select(async layerInfo =>
        {
            await semaphore.WaitAsync();
            try
            {
                var features = await ReadSingleLayerAsync(gdbPath, layerInfo.Index, layerInfo.Name);
                
                lock (lockObject)
                {
                    if (features.Count > 0)
                    {
                        allLayersData[layerInfo.Name] = features;
                    }
                    completedLayers++;
                    Console.WriteLine($"[Thread {Thread.CurrentThread.ManagedThreadId}] Completed layer '{layerInfo.Name}': {features.Count} features ({completedLayers}/{layerInfos.Count})");
                }
            }
            finally
            {
                semaphore.Release();
            }
        });
        
        await Task.WhenAll(tasks);
        
        Console.WriteLine($"Successfully read {allLayersData.Count} layers using {maxConcurrency} concurrent threads\n");
        
        return allLayersData;
    }
    
    private static readonly object ConsoleWriteLock = new object();
    
    public static List<LayerInfo> GetLayerInfos(string gdbPath)
    {
        var layerInfos = new List<LayerInfo>();
        
        using var dataSource = Ogr.Open(gdbPath, 0);
        if (dataSource == null)
        {
            throw new InvalidOperationException($"Failed to open GDB at path: {gdbPath}");
        }

        var layerCount = dataSource.GetLayerCount();
        Console.WriteLine($"Found {layerCount} layers in GDB");

        for (int layerIndex = 0; layerIndex < layerCount; layerIndex++)
        {
            var layer = dataSource.GetLayerByIndex(layerIndex);
            var layerName = layer.GetName();
            var featureCount = layer.GetFeatureCount(0);
            
            if (featureCount > 0)
            {
                layerInfos.Add(new LayerInfo
                {
                    LayerName = layerName,
                    TableName = "", // Will be set by caller
                    TotalFeatures = featureCount,
                    LayerIndex = layerIndex
                });
                Console.WriteLine($"Layer '{layerName}': {featureCount} features");
            }
        }
        
        return layerInfos;
    }
    
    public static async Task ProduceLayerBatchesAsync(string gdbPath, LayerInfo layerInfo, 
        ChannelWriter<LayerBatch> writer, StreamingProgress progress, int batchSize = 5000)
    {
        await Task.Run(async () =>
        {
            try
            {
                using var dataSource = Ogr.Open(gdbPath, 0);
                if (dataSource == null)
                {
                    throw new InvalidOperationException($"Failed to open GDB at path: {gdbPath} for layer {layerInfo.LayerName}");
                }
                
                var layer = dataSource.GetLayerByIndex(layerInfo.LayerIndex);
                var featureDefn = layer.GetLayerDefn();
                var fieldCount = featureDefn.GetFieldCount();
                
                layer.ResetReading();
                
                var batch = new List<Dictionary<string, object?>>();
                var processedCount = 0L;
                var isFirstBatch = true;
                
                Feature feature;
                while ((feature = layer.GetNextFeature()) != null)
                {
                    var featureData = new Dictionary<string, object?>();
                    
                    // Add FID
                    featureData["FID"] = feature.GetFID();
                    
                    // Add all fields
                    for (int i = 0; i < fieldCount; i++)
                    {
                        var fieldDefn = featureDefn.GetFieldDefn(i);
                        var fieldName = fieldDefn.GetName();
                        var fieldType = fieldDefn.GetFieldType();
                        
                        if (!feature.IsFieldSet(i))
                        {
                            featureData[fieldName] = null;
                            continue;
                        }

                        object? value = fieldType switch
                        {
                            FieldType.OFTInteger => feature.GetFieldAsInteger(i),
                            FieldType.OFTInteger64 => feature.GetFieldAsInteger64(i),
                            FieldType.OFTReal => feature.GetFieldAsDouble(i),
                            FieldType.OFTString => feature.GetFieldAsString(i),
                            FieldType.OFTDate => feature.GetFieldAsString(i),
                            FieldType.OFTDateTime => feature.GetFieldAsString(i),
                            _ => feature.GetFieldAsString(i)
                        };
                        
                        featureData[fieldName] = value;
                    }
                    
                    // Add geometry if present
                    var geometry = feature.GetGeometryRef();
                    if (geometry != null)
                    {
                        try
                        {
                            string wkt;
                            geometry.ExportToWkt(out wkt);
                            
                            if (IsWindows)
                            {
                                int srid = 4283;
                                var sqlGeog = SqlGeography.STGeomFromText(new SqlChars(wkt), srid);
                                
                                if (!sqlGeog.STIsValid().Value)
                                {
                                    sqlGeog = sqlGeog.MakeValid();
                                }
                                
                                featureData["GEOMETRY"] = sqlGeog;
                            }
                            else
                            {
                                featureData["WKT_GEOMETRY"] = wkt;
                                featureData["SRID"] = 4283;
                            }
                        }
                        catch (Exception ex)
                        {
                            lock (ConsoleWriteLock)
                            {
                                Console.WriteLine($"    Warning: Failed to convert geometry for feature {feature.GetFID()} in layer {layerInfo.LayerName}: {ex.Message}");
                            }
                            if (!IsWindows)
                            {
                                featureData["WKT_GEOMETRY"] = null;
                                featureData["SRID"] = 4283;
                            }
                            else
                            {
                                featureData["GEOMETRY"] = null;
                            }
                        }
                    }
                    
                    batch.Add(featureData);
                    feature.Dispose();
                    processedCount++;
                    
                    // Send batch when it reaches the batch size
                    if (batch.Count >= batchSize)
                    {
                        lock (ConsoleWriteLock)
                        {
                            Console.WriteLine($"[Producer] Sending batch of {batch.Count} features for layer '{layerInfo.LayerName}' ({processedCount}/{layerInfo.TotalFeatures})");
                        }
                        
                        await writer.WriteAsync(new LayerBatch
                        {
                            LayerName = layerInfo.LayerName,
                            TableName = layerInfo.TableName,
                            Features = batch,
                            IsFirstBatch = isFirstBatch,
                            IsLastBatch = false
                        });
                        
                        progress.UpdateProgress(layerInfo.LayerName, processedCount, layerInfo.TotalFeatures);
                        batch = new List<Dictionary<string, object?>>();
                        isFirstBatch = false;
                    }
                }
                
                // Send final batch if there are remaining features
                if (batch.Count > 0)
                {
                    lock (ConsoleWriteLock)
                    {
                        Console.WriteLine($"[Producer] Sending final batch of {batch.Count} features for layer '{layerInfo.LayerName}'");
                    }
                    
                    await writer.WriteAsync(new LayerBatch
                    {
                        LayerName = layerInfo.LayerName,
                        TableName = layerInfo.TableName,
                        Features = batch,
                        IsFirstBatch = isFirstBatch,
                        IsLastBatch = true
                    });
                }
                else if (processedCount > 0)
                {
                    lock (ConsoleWriteLock)
                    {
                        Console.WriteLine($"[Producer] Sending completion marker for layer '{layerInfo.LayerName}'");
                    }
                    
                    // Mark the previous batch as the last batch
                    await writer.WriteAsync(new LayerBatch
                    {
                        LayerName = layerInfo.LayerName,
                        TableName = layerInfo.TableName,
                        Features = new List<Dictionary<string, object?>>(),
                        IsFirstBatch = false,
                        IsLastBatch = true
                    });
                }
                
                progress.UpdateProgress(layerInfo.LayerName, processedCount, layerInfo.TotalFeatures);
                
                lock (ConsoleWriteLock)
                {
                    Console.WriteLine($"[Thread {Thread.CurrentThread.ManagedThreadId}] Producer completed layer '{layerInfo.LayerName}': {processedCount} features");
                }
            }
            catch (Exception ex)
            {
                lock (ConsoleWriteLock)
                {
                    Console.WriteLine($"Error reading layer {layerInfo.LayerName}: {ex.Message}");
                }
                throw;
            }
        });
    }
    
    private static async Task<List<Dictionary<string, object?>>> ReadSingleLayerAsync(string gdbPath, int layerIndex, string layerName)
    {
        return await Task.Run(() =>
        {
            var features = new List<Dictionary<string, object?>>();
            
            using var dataSource = Ogr.Open(gdbPath, 0);
            if (dataSource == null)
            {
                throw new InvalidOperationException($"Failed to open GDB at path: {gdbPath} for layer {layerName}");
            }
            
            var layer = dataSource.GetLayerByIndex(layerIndex);
            var featureDefn = layer.GetLayerDefn();
            var fieldCount = featureDefn.GetFieldCount();
            
            layer.ResetReading();
            Feature feature;
            
            while ((feature = layer.GetNextFeature()) != null)
            {
                var featureData = new Dictionary<string, object?>();
                
                // Add FID
                featureData["FID"] = feature.GetFID();
                
                // Add all fields
                for (int i = 0; i < fieldCount; i++)
                {
                    var fieldDefn = featureDefn.GetFieldDefn(i);
                    var fieldName = fieldDefn.GetName();
                    var fieldType = fieldDefn.GetFieldType();
                    
                    if (!feature.IsFieldSet(i))
                    {
                        featureData[fieldName] = null;
                        continue;
                    }

                    object? value = fieldType switch
                    {
                        FieldType.OFTInteger => feature.GetFieldAsInteger(i),
                        FieldType.OFTInteger64 => feature.GetFieldAsInteger64(i),
                        FieldType.OFTReal => feature.GetFieldAsDouble(i),
                        FieldType.OFTString => feature.GetFieldAsString(i),
                        FieldType.OFTDate => feature.GetFieldAsString(i), // DateTime as string
                        FieldType.OFTDateTime => feature.GetFieldAsString(i), // DateTime as string
                        _ => feature.GetFieldAsString(i)
                    };
                    
                    featureData[fieldName] = value;
                }
                
                // Add geometry if present
                var geometry = feature.GetGeometryRef();
                if (geometry != null)
                {
                    try
                    {
                        string wkt;
                        geometry.ExportToWkt(out wkt);
                        
                        if (IsWindows)
                        {
                            // Use SQL Geography on Windows
                            // Use SRID 4283 (GDA94) for Australian data
                            int srid = 4283;
                            
                            // Create SqlGeography from WKT
                            var sqlGeog = SqlGeography.STGeomFromText(new SqlChars(wkt), srid);
                            
                            // Ensure valid geography
                            if (!sqlGeog.STIsValid().Value)
                            {
                                sqlGeog = sqlGeog.MakeValid();
                            }
                            
                            featureData["GEOMETRY"] = sqlGeog;
                        }
                        else
                        {
                            // Use WKT on Linux
                            featureData["WKT_GEOMETRY"] = wkt;
                            featureData["SRID"] = 4283;
                        }
                    }
                    catch (Exception ex)
                    {
                        lock (ConsoleWriteLock)
                        {
                            Console.WriteLine($"    Warning: Failed to convert geometry for feature {feature.GetFID()} in layer {layerName}: {ex.Message}");
                        }
                        if (!IsWindows)
                        {
                            featureData["WKT_GEOMETRY"] = null;
                            featureData["SRID"] = 4283;
                        }
                        else
                        {
                            featureData["GEOMETRY"] = null;
                        }
                    }
                }
                
                features.Add(featureData);
                feature.Dispose();
            }
            
            return features;
        });
    }
}