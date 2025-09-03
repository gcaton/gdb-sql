using MaxRev.Gdal.Core;
using Microsoft.SqlServer.Types;
using OSGeo.OGR;
using System.Data.SqlTypes;
using System.Runtime.InteropServices;

namespace GdbToSql;

public class GdbReader
{
    private static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
    
    static GdbReader()
    {
        GdalBase.ConfigureAll();
    }

    public static Dictionary<string, List<Dictionary<string, object?>>> ReadAllGdbLayers(string gdbPath)
    {
        var allLayersData = new Dictionary<string, List<Dictionary<string, object?>>>();
        
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
            var features = new List<Dictionary<string, object?>>();
            
            Console.Write($"Reading layer '{layerName}'... ");
            
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
                        Console.WriteLine($"    Warning: Failed to convert geometry for feature {feature.GetFID()}: {ex.Message}");
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
            
            Console.WriteLine($"Found {features.Count} features");
            
            if (features.Count > 0)
            {
                allLayersData[layerName] = features;
            }
        }
        
        return allLayersData;
    }
}