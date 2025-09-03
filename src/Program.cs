using GdbToSql;
using Microsoft.Extensions.Configuration;
using System.Runtime.InteropServices;
using System.Threading.Channels;

try
{
    var totalTimer = System.Diagnostics.Stopwatch.StartNew();
    var timings = new Dictionary<string, TimeSpan>();
    
    Console.WriteLine("GDB to SQL Converter");
    Console.WriteLine("====================");
    
    // Display platform and geometry format
    var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
    Console.WriteLine($"Platform: {RuntimeInformation.OSDescription}");
    Console.WriteLine($"Geometry format: {(isWindows ? "SQL Geography" : "WKT with SRID column")}");
    Console.WriteLine();
    
    // Load configuration
    var configTimer = System.Diagnostics.Stopwatch.StartNew();
    var configuration = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
        .Build();
    
    var settings = configuration.Get<AppSettings>();
    configTimer.Stop();
    timings["Configuration Load"] = configTimer.Elapsed;
    
    if (settings == null)
    {
        throw new InvalidOperationException("Failed to load configuration from appsettings.json");
    }
    
    // Validate settings
    if (string.IsNullOrEmpty(settings.ConnectionStrings.DefaultConnection))
        throw new ArgumentException("Connection string is not configured");
    if (string.IsNullOrEmpty(settings.GdbToSql.SourceGdbPath))
        throw new ArgumentException("Source GDB path is not configured");
    
    Console.WriteLine($"Reading from GDB: {settings.GdbToSql.SourceGdbPath}");
    Console.WriteLine($"Table prefix: {settings.GdbToSql.TargetTablePrefix}");
    Console.WriteLine();
    
    // Test database connection first
    var dbTestTimer = System.Diagnostics.Stopwatch.StartNew();
    Console.WriteLine("Testing database connection...");
    var connectionValid = await ConnectionTester.TestDatabaseConnectionAsync(settings.ConnectionStrings.DefaultConnection);
    dbTestTimer.Stop();
    timings["Database Connection Test"] = dbTestTimer.Elapsed;
    
    if (!connectionValid)
    {
        Console.WriteLine("Database connection failed. Cannot continue.");
        return;
    }
    
    Console.WriteLine("Database connection successful. Continuing with processing...\n");
    
    // Get layer information
    var layerInfoTimer = System.Diagnostics.Stopwatch.StartNew();
    Console.WriteLine("Getting layer information from GDB...");
    var layerInfos = GdbReader.GetLayerInfos(settings.GdbToSql.SourceGdbPath);
    layerInfoTimer.Stop();
    timings["Layer Information Scan"] = layerInfoTimer.Elapsed;
    Console.WriteLine($"GetLayerInfos returned {layerInfos.Count} layers");
    
    if (layerInfos.Count == 0)
    {
        Console.WriteLine("No layers with features found in GDB");
        return;
    }
    
    // Set table names
    foreach (var layerInfo in layerInfos)
    {
        layerInfo.TableName = $"{settings.GdbToSql.TargetTablePrefix}{layerInfo.LayerName}";
    }
    
    var totalFeatures = layerInfos.Sum(l => l.TotalFeatures);
    Console.WriteLine($"\nFound {layerInfos.Count} layers with {totalFeatures} total features to process");
    Console.WriteLine("Using streaming producer-consumer pattern for memory efficiency");
    Console.WriteLine();
    
    if (layerInfos.Count == 0)
    {
        Console.WriteLine("No layers found - nothing to process");
        return;
    }
    
    // Create channel for streaming batches
    var channel = Channel.CreateUnbounded<LayerBatch>();
    var progress = new StreamingProgress();
    
    // Initialize progress tracking
    foreach (var layerInfo in layerInfos)
    {
        progress.UpdateProgress(layerInfo.LayerName, 0, layerInfo.TotalFeatures);
    }
    
    // Start progress reporting task
    using var cancellationTokenSource = new CancellationTokenSource();
    var progressTask = Task.Run(async () =>
    {
        try
        {
            while (!cancellationTokenSource.Token.IsCancellationRequested)
            {
                await Task.Delay(2000, cancellationTokenSource.Token);
                progress.ShowProgress();
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
    });
    
    // Start consumer tasks
    var consumerCount = Math.Min(Environment.ProcessorCount, 4); // Limit DB connections
    Console.WriteLine($"Starting {consumerCount} consumer threads...");
    
    var consumers = Enumerable.Range(0, consumerCount).Select(consumerId => Task.Run(async () =>
    {
        try
        {
            var sqlWriter = new SqlWriter(settings.ConnectionStrings.DefaultConnection);
            var batchesProcessed = 0;
            
            Console.WriteLine($"[Consumer {consumerId}] Started");
            
            await foreach (var batch in channel.Reader.ReadAllAsync())
            {
                try
                {
                    batchesProcessed++;
                    Console.WriteLine($"[Consumer {consumerId}] Processing batch {batchesProcessed} for layer '{batch.LayerName}' with {batch.Features.Count} features");
                    
                    await sqlWriter.ProcessStreamingBatchAsync(batch);
                    
                    Console.WriteLine($"[Consumer {consumerId}] Completed batch {batchesProcessed} for layer '{batch.LayerName}'");
                }
                catch (Exception batchEx)
                {
                    Console.WriteLine($"[Consumer {consumerId} ERROR] Failed to process batch {batchesProcessed}: {batchEx.Message}");
                    Console.WriteLine($"[Consumer {consumerId} ERROR] Stack trace: {batchEx.StackTrace}");
                    // Continue processing other batches rather than stopping the consumer
                }
            }
            
            Console.WriteLine($"[Consumer {consumerId}] Finished after processing {batchesProcessed} batches");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Consumer {consumerId} FATAL ERROR] Consumer failed: {ex.Message}");
            Console.WriteLine($"[Consumer {consumerId} FATAL ERROR] Stack trace: {ex.StackTrace}");
            throw;
        }
    })).ToArray();
    
    // Start producer tasks
    var producerCount = Math.Min(Environment.ProcessorCount, layerInfos.Count);
    var semaphore = new SemaphoreSlim(producerCount, producerCount);
    
    Console.WriteLine($"Starting {producerCount} producer threads for {layerInfos.Count} layers...");
    
    // Calculate optimal batch size based on layer characteristics
    var optimalBatchSize = CalculateOptimalBatchSize(layerInfos);
    Console.WriteLine($"Using dynamic batch size: {optimalBatchSize} features per batch");
    
    // Monitor memory usage
    GC.Collect();
    var initialMemory = GC.GetTotalMemory(false) / (1024 * 1024);
    Console.WriteLine($"Initial memory usage: {initialMemory} MB");
    
    var producers = layerInfos.Select(async layerInfo =>
    {
        await semaphore.WaitAsync();
        try
        {
            Console.WriteLine($"[Producer] Starting layer '{layerInfo.LayerName}' with {layerInfo.TotalFeatures} features");
            await GdbReader.ProduceLayerBatchesAsync(settings.GdbToSql.SourceGdbPath, 
                layerInfo, channel.Writer, progress, batchSize: optimalBatchSize);
            Console.WriteLine($"[Producer] Completed layer '{layerInfo.LayerName}'");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Producer] Error processing layer '{layerInfo.LayerName}': {ex.Message}");
            throw;
        }
        finally
        {
            semaphore.Release();
        }
    });
    
    var processingTimer = System.Diagnostics.Stopwatch.StartNew();
    Console.WriteLine("Waiting for all producers to complete...");
    await Task.WhenAll(producers);
    var producerTime = processingTimer.Elapsed;
    timings["Producer Processing"] = producerTime;
    
    Console.WriteLine("All producers completed. Signaling completion to consumers...");
    // Signal completion to consumers
    channel.Writer.Complete();
    
    Console.WriteLine("Waiting for all consumers to complete...");
    // Wait for all consumers to complete
    await Task.WhenAll(consumers);
    processingTimer.Stop();
    timings["Total Data Processing"] = processingTimer.Elapsed;
    timings["Consumer Processing"] = processingTimer.Elapsed - producerTime;
    
    Console.WriteLine("All consumers completed. Stopping progress reporting...");
    
    // Stop progress reporting
    cancellationTokenSource.Cancel();
    
    // Final progress update
    progress.ShowProgress();
    
    // Final memory check
    GC.Collect();
    var finalMemory = GC.GetTotalMemory(false) / (1024 * 1024);
    Console.WriteLine($"\nFinal memory usage: {finalMemory} MB (delta: {finalMemory - initialMemory} MB)");
    
    totalTimer.Stop();
    timings["Total Application Time"] = totalTimer.Elapsed;
    
    Console.WriteLine($"\nSuccessfully processed {layerInfos.Count} layers with {totalFeatures} total features using streaming pattern");
    Console.WriteLine($"Used {producerCount} producer threads and {consumerCount} consumer threads with batch size {optimalBatchSize}");
    
    // Display timing summary
    Console.WriteLine("\n" + new string('=', 50));
    Console.WriteLine("PERFORMANCE TIMING SUMMARY");
    Console.WriteLine(new string('=', 50));
    foreach (var timing in timings.OrderBy(t => t.Value))
    {
        Console.WriteLine($"{timing.Key,-30}: {timing.Value.TotalSeconds:F2}s");
    }
    Console.WriteLine(new string('=', 50));
    Console.WriteLine($"{"Features per second",-30}: {totalFeatures / timings["Total Data Processing"].TotalSeconds:F0}");
    Console.WriteLine($"{"MB processed per second",-30}: {finalMemory / timings["Total Data Processing"].TotalSeconds:F2}");
    Console.WriteLine(new string('=', 50));
}
catch (Exception ex)
{
    Console.WriteLine($"\nError: {ex.Message}");
    if (ex.InnerException != null)
    {
        Console.WriteLine($"Inner error: {ex.InnerException.Message}");
    }
    Environment.Exit(1);
}

static int CalculateOptimalBatchSize(List<LayerInfo> layerInfos)
{
    var totalFeatures = layerInfos.Sum(l => l.TotalFeatures);
    var avgFeaturesPerLayer = totalFeatures / layerInfos.Count;
    
    // Dynamic batch sizing based on data volume
    return avgFeaturesPerLayer switch
    {
        < 1000 => 500,          // Small layers: smaller batches
        < 10000 => 2000,        // Medium layers: moderate batches  
        < 100000 => 5000,       // Large layers: larger batches
        _ => 10000              // Very large layers: maximum batches
    };
}
