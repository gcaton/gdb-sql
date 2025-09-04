using MaxRev.Gdal.Core;
using OSGeo.OGR;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Spectre.Console;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using NetTopologySuite.Operation.Valid;
using NetTopologySuite;
using NetTopologySuite.Geometries.Implementation;

namespace GdbToSql;

public static class SpatialHelpers
{
    private static int GDA94_SRID = 4283; // GDA94 SRID

    // While this declaration doesn't appear to be used there seems to be a bug that means
    // if this line isn't here it can cause problems.
    // https://github.com/NetTopologySuite/NetTopologySuite/issues/573
    private static readonly GeometryFactory DefaultFactory = GeometryFactory.Default;

    public static readonly PrecisionModel DefaultPrecisionModel = new();
    public static readonly CoordinateSequenceFactory XYCoordinateSequenceFactory = new DotSpatialAffineCoordinateSequenceFactory(Ordinates.XY);
    public static readonly NtsGeometryServices Gda94GeometryService = new(XYCoordinateSequenceFactory, DefaultPrecisionModel, GDA94_SRID);
    public static readonly GeometryFactory Gda94GeometryFactory = new GeometryFactoryEx(DefaultPrecisionModel, GDA94_SRID, XYCoordinateSequenceFactory, Gda94GeometryService)
    {
        OrientationOfExteriorRing = LinearRingOrientation.CCW
    };

    public static readonly SqlServerBytesWriter SqlServerBytesWriter = new ()
    {
        HandleOrdinates = Ordinates.XY,
        IsGeography = true
    };


    public static bool IsPolygon(this NetTopologySuite.Geometries.Geometry shape)
    {
        return shape != null && shape.GeometryType.Equals("polygon", StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsMultiPolygon(this NetTopologySuite.Geometries.Geometry shape)
    {
        return shape != null && shape.GeometryType.Equals("multipolygon", StringComparison.OrdinalIgnoreCase);
    }

    public static bool IsGeometryCollection(this NetTopologySuite.Geometries.Geometry shape)
    {
        return shape != null && shape.GeometryType.Equals("geometrycollection", StringComparison.OrdinalIgnoreCase);
    }

    public static NetTopologySuite.Geometries.Geometry? CorrectPolygonOrientation(this NetTopologySuite.Geometries.Geometry shape)
    {
        // We only care about non empty geometries that are or can contain polygons.
        if (shape == null || shape.IsEmpty || (!shape.IsPolygon() && !shape.IsGeometryCollection() && !shape.IsMultiPolygon())) { return shape; }

        // Deal with the simple case first, if the shape is a polygon, check is orientation and reverse if nessessary.
        if (shape.IsPolygon())
        {
            // reverse any non ccw shells
            var poly = (Polygon)shape;
            if (!poly.Shell.IsCCW)
            {
                var lr = (LinearRing)poly.Shell.Reverse();
                poly = Gda94GeometryFactory.CreatePolygon(lr, poly.Holes);

                // Debug.Assert(!poly.Shell.IsCCW, "The shell of this poly has just been reversed, but it remains counter clockwise.");
            }

            // all holes are required to be clockwise.
            if (poly.Holes.Length > 0)
            {
                var cwHoles = new List<LinearRing>();

                for (var i = 0; i < poly.Holes.Length; i++)
                {
                    if (poly.Holes[i].IsCCW)
                    {
                        var cwHole = (LinearRing)poly.Holes[i].Reverse();
                        cwHoles.Add(cwHole);

                    }
                    else
                    {
                        cwHoles.Add(poly.Holes[i]);
                    }
                }

                poly = Gda94GeometryFactory.CreatePolygon(poly.Shell, cwHoles.ToArray());
            }

            poly.SRID = GDA94_SRID;

            return poly;
        }

        // If we have a multipolygon, look at all polygons within.
        if (shape.IsMultiPolygon())
        {
            var multipolygon = (MultiPolygon)shape;
            for (var i = 0; i < multipolygon.Geometries.Length; i++)
            {
                var poly = (Polygon)multipolygon.Geometries[i];
                multipolygon.Geometries.SetValue(poly.CorrectPolygonOrientation(), i);
            }
        }

        // If we have a geom collection, look at all polygons and multi-polygons within.
        if (shape.IsGeometryCollection())
        {
            var geomCollection = (GeometryCollection)shape;

            for (var i = 0; i < geomCollection.Geometries.Length; i++)
            {
                var geom = geomCollection[i];

                if (geom.IsPolygon() || geom.IsMultiPolygon())
                {
                    geomCollection.Geometries.SetValue(geom.CorrectPolygonOrientation(), i);
                }
            }
        }

        return shape;
    }
}

public class GdbReader
{
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
            // Found layers

            for (int layerIndex = 0; layerIndex < layerCount; layerIndex++)
            {
                var layer = dataSource.GetLayerByIndex(layerIndex);
                var layerName = layer.GetName();
                var featureCount = layer.GetFeatureCount(0);
                
                if (featureCount > 0)
                {
                    layerInfos.Add((layerIndex, layerName, featureCount));
                    // Layer info collected
                }
            }
        }
        
        if (layerInfos.Count == 0)
        {
            return new Dictionary<string, List<Dictionary<string, object?>>>();
        }
        
        // Reading layers in parallel
        
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
                    // Layer completed
                }
            }
            finally
            {
                semaphore.Release();
            }
        });
        
        await Task.WhenAll(tasks);
        
        // All layers read successfully
        
        return allLayersData;
    }
    
    public static List<LayerInfo> GetLayerInfos(string gdbPath)
    {
        var layerInfos = new List<LayerInfo>();
        
        using var dataSource = Ogr.Open(gdbPath, 0);
        if (dataSource == null)
        {
            throw new InvalidOperationException($"Failed to open GDB at path: {gdbPath}");
        }

        var layerCount = dataSource.GetLayerCount();
        // Found layers

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
                // Layer found for streaming
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
                        int srid = 4283;
                        geometry.ExportToWkt(out var wkt);
                        if (wkt == null)
                        {
                            featureData["SHAPE"] = null;
                        }
                        else
                        {
                            var ntsReader = new WKTReader(SpatialHelpers.Gda94GeometryService);
                            var ntsGeometry = ntsReader.Read(wkt);
                            ntsGeometry.SRID = srid;
                            ntsGeometry = ntsGeometry.CorrectPolygonOrientation();
                            featureData["SHAPE"] = SpatialHelpers.SqlServerBytesWriter.Write(ntsGeometry);
                            //featureData["SHAPE"] = ntsGeometry;
                        }
                    }
                    
                    batch.Add(featureData);
                    feature.Dispose();
                    processedCount++;
                    
                    // Send batch when it reaches the batch size
                    if (batch.Count >= batchSize)
                    {
                        // Sending batch silently
                        
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
                    // Final batch sent
                    
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
                    // Completion marker sent
                    
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
                
                // Producer completed layer
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine($"[red]✗ Error reading layer [bold]{layerInfo.LayerName}[/]: {ex.Message.EscapeMarkup()}[/]");
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
                    int srid = 4283;
                    geometry.ExportToWkt(out var wkt);
                    if (wkt == null)
                    {
                        featureData["SHAPE"] = null;
                    }
                    else
                    {
                        var ntsReader = new WKTReader(SpatialHelpers.Gda94GeometryService);
                        var ntsGeometry = ntsReader.Read(wkt);
                        ntsGeometry.SRID = srid;
                        ntsGeometry = ntsGeometry.CorrectPolygonOrientation();
                        featureData["SHAPE"] = SpatialHelpers.SqlServerBytesWriter.Write(ntsGeometry);
                    }
                }
                
                features.Add(featureData);
                feature.Dispose();
            }
            
            return features;
        });
    }


    

    //private static Geometry? EnsureAntiClockwiseOrientation(Geometry? geometry)
    //{
    //    if (geometry == null)
    //        return geometry;

    //    var geometryType = geometry.GetGeometryType();

    //    switch (geometryType)
    //    {
    //        case wkbGeometryType.wkbPolygon:
    //            return CorrectPolygonOrientation(geometry);

    //        case wkbGeometryType.wkbMultiPolygon:
    //            return CorrectMultiPolygonOrientation(geometry);

    //        default:
    //            // For non-polygon geometries, return as-is
    //            return geometry;
    //    }
    //}

    //private static Geometry CorrectPolygonOrientation(Geometry polygon)
    //{
    //    try
    //    {
    //        // Get the exterior ring
    //        var exteriorRing = polygon.GetGeometryRef(0);
    //        if (exteriorRing == null)
    //            return polygon;

    //        // Check if exterior ring is clockwise (should be anti-clockwise)
    //        if (IsClockwise(exteriorRing))
    //        {
    //            // Create a new polygon with corrected orientation
    //            var correctedPolygon = new OSGeo.OGR.Geometry(wkbGeometryType.wkbPolygon);

    //            // Reverse exterior ring
    //            var reversedExterior = ReverseRing(exteriorRing);
    //            correctedPolygon.AddGeometry(reversedExterior);

    //            // Add interior rings (holes) - these should be clockwise
    //            int ringCount = polygon.GetGeometryCount();
    //            for (int i = 1; i < ringCount; i++)
    //            {
    //                var interiorRing = polygon.GetGeometryRef(i);
    //                if (!IsClockwise(interiorRing))
    //                {
    //                    // Interior ring should be clockwise, so reverse it
    //                    var reversedInterior = ReverseRing(interiorRing);
    //                    correctedPolygon.AddGeometry(reversedInterior);
    //                }
    //                else
    //                {
    //                    // Already clockwise, clone it
    //                    correctedPolygon.AddGeometry(interiorRing.Clone());
    //                }
    //            }

    //            return correctedPolygon;
    //        }
    //        else
    //        {
    //            // Exterior ring is already anti-clockwise, check interior rings
    //            bool needsCorrection = false;
    //            int ringCount = polygon.GetGeometryCount();

    //            for (int i = 1; i < ringCount; i++)
    //            {
    //                var interiorRing = polygon.GetGeometryRef(i);
    //                if (!IsClockwise(interiorRing))
    //                {
    //                    needsCorrection = true;
    //                    break;
    //                }
    //            }

    //            if (needsCorrection)
    //            {
    //                var correctedPolygon = new OSGeo.OGR.Geometry(wkbGeometryType.wkbPolygon);
    //                correctedPolygon.AddGeometry(exteriorRing.Clone());

    //                for (int i = 1; i < ringCount; i++)
    //                {
    //                    var interiorRing = polygon.GetGeometryRef(i);
    //                    if (!IsClockwise(interiorRing))
    //                    {
    //                        var reversedInterior = ReverseRing(interiorRing);
    //                        correctedPolygon.AddGeometry(reversedInterior);
    //                    }
    //                    else
    //                    {
    //                        correctedPolygon.AddGeometry(interiorRing.Clone());
    //                    }
    //                }

    //                return correctedPolygon;
    //            }

    //            return polygon; // Already correct
    //        }
    //    }
    //    catch (Exception ex)
    //    {
    //        Console.WriteLine($"Warning: Failed to correct polygon orientation: {ex.Message}");
    //        return polygon; // Return original on error
    //    }
    //}

    //private static Geometry CorrectMultiPolygonOrientation(Geometry multiPolygon)
    //{
    //    try
    //    {
    //        var correctedMultiPolygon = new OSGeo.OGR.Geometry(wkbGeometryType.wkbMultiPolygon);
    //        int polygonCount = multiPolygon.GetGeometryCount();

    //        for (int i = 0; i < polygonCount; i++)
    //        {
    //            var polygon = multiPolygon.GetGeometryRef(i);
    //            var correctedPolygon = CorrectPolygonOrientation(polygon);
    //            correctedMultiPolygon.AddGeometry(correctedPolygon);
    //        }

    //        return correctedMultiPolygon;
    //    }
    //    catch (Exception ex)
    //    {
    //        Console.WriteLine($"Warning: Failed to correct multi-polygon orientation: {ex.Message}");
    //        return multiPolygon; // Return original on error
    //    }
    //}

    //private static bool IsClockwise(Geometry ring)
    //{
    //    try
    //    {
    //        int pointCount = ring.GetPointCount();
    //        if (pointCount < 3)
    //            return false;

    //        double area = 0.0;

    //        for (int i = 0; i < pointCount - 1; i++)
    //        {
    //            double[] point1 = new double[2];
    //            double[] point2 = new double[2];

    //            ring.GetPoint(i, point1);
    //            ring.GetPoint(i + 1, point2);

    //            area += (point2[0] - point1[0]) * (point2[1] + point1[1]);
    //        }

    //        // Positive area indicates clockwise orientation
    //        return area > 0;
    //    }
    //    catch (Exception)
    //    {
    //        return false; // Assume counter-clockwise on error
    //    }
    //}

    //private static Geometry ReverseRing(Geometry ring)
    //{
    //    try
    //    {
    //        var reversedRing = new OSGeo.OGR.Geometry(wkbGeometryType.wkbLinearRing);
    //        int pointCount = ring.GetPointCount();

    //        // Add points in reverse order
    //        for (int i = pointCount - 1; i >= 0; i--)
    //        {
    //            double[] point = new double[3]; // x, y, z
    //            ring.GetPoint(i, point);
    //            reversedRing.AddPoint(point[0], point[1], point[2]);
    //        }

    //        return reversedRing;
    //    }
    //    catch (Exception ex)
    //    {
    //        Console.WriteLine($"Warning: Failed to reverse ring: {ex.Message}");
    //        return ring.Clone(); // Return clone of original on error
    //    }
    //}
}