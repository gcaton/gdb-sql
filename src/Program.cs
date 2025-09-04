using GdbToSql;
using Microsoft.Extensions.Configuration;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Spectre.Console;

try
{
    var totalTimer = System.Diagnostics.Stopwatch.StartNew();
    var timings = new Dictionary<string, TimeSpan>();
    
    // Create a nice header
    var rule = new Rule("[bold blue]GDB to SQL Converter[/]");
    rule.Style = Style.Parse("blue");
    AnsiConsole.Write(rule);
    AnsiConsole.WriteLine();
    
    // Display platform and geometry format in a nice panel
    var panel = new Panel(new Markup($"""
        [bold yellow]Platform:[/] {RuntimeInformation.OSDescription}
        [bold yellow]Geometry Format:[/] [green]SQL Geography[/]
        [bold yellow]Multi-threading:[/] [green]Enabled[/]
        [bold yellow]Memory optimization:[/] [green]Producer-Consumer Pattern[/]
        """))
    {
        Header = new PanelHeader("[bold green]System Information[/]"),
        Border = BoxBorder.Rounded,
        BorderStyle = Style.Parse("green")
    };
    AnsiConsole.Write(panel);
    AnsiConsole.WriteLine();
    
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
    
    // Display configuration in a nice table
    var configTable = new Table()
        .BorderColor(Color.Grey)
        .Border(TableBorder.Rounded)
        .AddColumn(new TableColumn("[bold yellow]Setting[/]"))
        .AddColumn(new TableColumn("[bold cyan]Value[/]"))
        .AddRow("GDB Source", $"[green]{settings.GdbToSql.SourceGdbPath}[/]")
        .AddRow("Table Prefix", $"[green]{settings.GdbToSql.TargetTablePrefix}[/]");
        
    AnsiConsole.Write(configTable);
    AnsiConsole.WriteLine();
    
    // Test database connection first
    var dbTestTimer = System.Diagnostics.Stopwatch.StartNew();
    
    await AnsiConsole.Status()
        .Spinner(Spinner.Known.Star)
        .SpinnerStyle(Style.Parse("green bold"))
        .StartAsync("[yellow]Testing database connection...[/]", async ctx =>
        {
            var connectionValid = await ConnectionTester.TestDatabaseConnectionAsync(settings.ConnectionStrings.DefaultConnection);
            dbTestTimer.Stop();
            timings["Database Connection Test"] = dbTestTimer.Elapsed;
            
            if (!connectionValid)
            {
                AnsiConsole.MarkupLine("[bold red]✗ Database connection failed. Cannot continue.[/]");
                Environment.Exit(1);
            }
            
            AnsiConsole.MarkupLine("[bold green]✓ Database connection successful![/]");
        });
    
    // Get layer information
    var layerInfoTimer = System.Diagnostics.Stopwatch.StartNew();
    List<LayerInfo> layerInfos = null!;
    
    await AnsiConsole.Status()
        .Spinner(Spinner.Known.Dots)
        .SpinnerStyle(Style.Parse("cyan bold"))
        .StartAsync("[yellow]Scanning GDB layers...[/]", async ctx =>
        {
            layerInfos = await Task.Run(() => GdbReader.GetLayerInfos(settings.GdbToSql.SourceGdbPath));
            layerInfoTimer.Stop();
            timings["Layer Information Scan"] = layerInfoTimer.Elapsed;
        });
    
    if (layerInfos.Count == 0)
    {
        AnsiConsole.MarkupLine("[bold red]✗ No layers with features found in GDB[/]");
        return;
    }
    
    AnsiConsole.MarkupLine($"[bold green]✓ Found {layerInfos.Count} layers with features[/]");
    
    // Set table names
    foreach (var layerInfo in layerInfos)
    {
        layerInfo.TableName = $"{settings.GdbToSql.TargetTablePrefix}{layerInfo.LayerName}";
    }
    
    var totalFeatures = layerInfos.Sum(l => l.TotalFeatures);
    
    // Display layer summary in a nice table
    var layerTable = new Table()
        .BorderColor(Color.Grey)
        .Border(TableBorder.Rounded)
        .AddColumn(new TableColumn("[bold yellow]Layer[/]").LeftAligned())
        .AddColumn(new TableColumn("[bold cyan]Features[/]").RightAligned())
        .AddColumn(new TableColumn("[bold green]Target Table[/]").LeftAligned());
    
    foreach (var layer in layerInfos.Take(10)) // Show first 10 layers
    {
        layerTable.AddRow(layer.LayerName, layer.TotalFeatures.ToString("N0"), layer.TableName);
    }
    
    if (layerInfos.Count > 10)
    {
        layerTable.AddRow($"[dim]... and {layerInfos.Count - 10} more layers[/]", "[dim]...[/]", "[dim]...[/]");
    }
    
    AnsiConsole.Write(layerTable);
    
    // Summary panel
    var summaryPanel = new Panel(new Markup($"""
        [bold yellow]Total Layers:[/] {layerInfos.Count:N0}
        [bold yellow]Total Features:[/] {totalFeatures:N0}
        [bold yellow]Processing Pattern:[/] [green]Streaming Producer-Consumer[/]
        [bold yellow]Memory Optimization:[/] [green]Enabled[/]
        """))
    {
        Header = new PanelHeader("[bold green]Processing Summary[/]"),
        Border = BoxBorder.Rounded,
        BorderStyle = Style.Parse("green")
    };
    AnsiConsole.Write(summaryPanel);
    
    if (layerInfos.Count == 0)
    {
        AnsiConsole.MarkupLine("[bold red]✗ No layers found - nothing to process[/]");
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
    
    // Start progress reporting task with Spectre.Console
    using var cancellationTokenSource = new CancellationTokenSource();
    var progressTask = Task.Run(async () =>
    {
        try
        {
            while (!cancellationTokenSource.Token.IsCancellationRequested)
            {
                await Task.Delay(3000, cancellationTokenSource.Token);
                
                // Update progress display
                var processed = progress.GetTotalProcessed();
                var percentage = totalFeatures > 0 ? (double)processed / totalFeatures * 100 : 0;
                
                AnsiConsole.MarkupLine($"[yellow]Progress:[/] [green]{processed:N0}[/] / [cyan]{totalFeatures:N0}[/] features ([green]{percentage:F1}%[/])");
            }
        }
        catch (OperationCanceledException)
        {
            // Expected when cancellation is requested
        }
    });
    
    // Start consumer tasks
    var consumerCount = Math.Min(Environment.ProcessorCount, 4); // Limit DB connections
    
    var consumers = Enumerable.Range(0, consumerCount).Select(consumerId => Task.Run(async () =>
    {
        try
        {
            var sqlWriter = new SqlWriter(settings.ConnectionStrings.DefaultConnection);
            var batchesProcessed = 0;
            
            // Reduced logging for cleaner Spectre.Console output
            
            await foreach (var batch in channel.Reader.ReadAllAsync())
            {
                try
                {
                    batchesProcessed++;
                    // Only log significant events or errors
                    
                    await sqlWriter.ProcessStreamingBatchAsync(batch);
                }
                catch (Exception batchEx)
                {
                    AnsiConsole.MarkupLine($"[red]✗ Consumer {consumerId} failed batch {batchesProcessed}: {batchEx.Message.EscapeMarkup()}[/]");
                    // Continue processing other batches rather than stopping the consumer
                }
            }
            
            AnsiConsole.MarkupLine($"[green]✓ Consumer {consumerId} finished processing {batchesProcessed} batches[/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[bold red]✗ Consumer {consumerId} fatal error: {ex.Message.EscapeMarkup()}[/]");
            throw;
        }
    })).ToArray();
    
    // Start producer tasks
    var producerCount = Math.Min(Environment.ProcessorCount, layerInfos.Count);
    var semaphore = new SemaphoreSlim(producerCount, producerCount);
    
    AnsiConsole.MarkupLine($"[yellow]Starting[/] [green]{consumerCount} consumer threads[/] [yellow]and[/] [green]{producerCount} producer threads[/]...");
    
    // Calculate optimal batch size based on layer characteristics
    var optimalBatchSize = CalculateOptimalBatchSize(layerInfos);
    
    AnsiConsole.MarkupLine($"[yellow]Using dynamic batch size:[/] [green]{optimalBatchSize:N0} features per batch[/]");
    
    // Monitor memory usage
    GC.Collect();
    var initialMemory = GC.GetTotalMemory(false) / (1024 * 1024);
    AnsiConsole.MarkupLine($"[yellow]Initial memory usage:[/] [green]{initialMemory} MB[/]");
    AnsiConsole.WriteLine();
    
    var producers = layerInfos.Select(async layerInfo =>
    {
        await semaphore.WaitAsync();
        try
        {
            AnsiConsole.MarkupLine($"[cyan]→ Processing layer [bold]{layerInfo.LayerName}[/] ({layerInfo.TotalFeatures:N0} features)[/]");
            await GdbReader.ProduceLayerBatchesAsync(settings.GdbToSql.SourceGdbPath, 
                layerInfo, channel.Writer, progress, batchSize: optimalBatchSize);
            AnsiConsole.MarkupLine($"[green]✓ Completed layer [bold]{layerInfo.LayerName}[/][/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]✗ Producer error on layer [bold]{layerInfo.LayerName}[/]: {ex.Message.EscapeMarkup()}[/]");
            throw;
        }
        finally
        {
            semaphore.Release();
        }
    });
    
    var processingTimer = System.Diagnostics.Stopwatch.StartNew();
    
    // Process all layers silently
    await Task.WhenAll(producers);
    
    var producerTime = processingTimer.Elapsed;
    timings["Producer Processing"] = producerTime;
    
    AnsiConsole.MarkupLine("[green]✓ All producers completed. Finalizing data import...[/]");
    // Signal completion to consumers
    channel.Writer.Complete();
    
    await AnsiConsole.Status()
        .Spinner(Spinner.Known.BouncingBall)
        .SpinnerStyle(Style.Parse("cyan bold"))
        .StartAsync("[cyan]Finalizing database operations...[/]", async ctx =>
        {
            await Task.WhenAll(consumers);
        });
    
    processingTimer.Stop();
    timings["Total Data Processing"] = processingTimer.Elapsed;
    timings["Consumer Processing"] = processingTimer.Elapsed - producerTime;
    
    // Stop progress reporting
    cancellationTokenSource.Cancel();
    
    // Final progress update
    progress.ShowProgress();
    
    // Final memory check
    GC.Collect();
    var finalMemory = GC.GetTotalMemory(false) / (1024 * 1024);
    AnsiConsole.MarkupLine($"[yellow]Final memory usage:[/] [green]{finalMemory} MB[/] [dim](delta: {finalMemory - initialMemory:+#;-#;0} MB)[/]");
    
    totalTimer.Stop();
    timings["Total Application Time"] = totalTimer.Elapsed;
    
    // Success message
    AnsiConsole.WriteLine();
    var successPanel = new Panel(new Markup($"""
        [bold green]✓ Import completed successfully![/]
        
        [bold yellow]Processed:[/] {layerInfos.Count:N0} layers with {totalFeatures:N0} total features
        [bold yellow]Pattern:[/] Streaming producer-consumer architecture  
        [bold yellow]Threads:[/] {producerCount} producers, {consumerCount} consumers
        [bold yellow]Batch Size:[/] {optimalBatchSize:N0} features per batch
        """))
    {
        Header = new PanelHeader("[bold green]Import Complete[/]"),
        Border = BoxBorder.Double,
        BorderStyle = Style.Parse("green bold")
    };
    AnsiConsole.Write(successPanel);
    
    // Display timing summary with Spectre.Console
    AnsiConsole.WriteLine();
    var timingRule = new Rule("[bold green]Performance Summary[/]");
    timingRule.Style = Style.Parse("green");
    AnsiConsole.Write(timingRule);
    
    // Create timing table
    var timingTable = new Table()
        .BorderColor(Color.Green)
        .Border(TableBorder.Rounded)
        .AddColumn(new TableColumn("[bold yellow]Phase[/]").LeftAligned())
        .AddColumn(new TableColumn("[bold cyan]Duration[/]").RightAligned())
        .AddColumn(new TableColumn("[bold green]Percentage[/]").RightAligned());
    
    var totalTime = timings["Total Application Time"].TotalSeconds;
    foreach (var timing in timings.OrderByDescending(t => t.Value))
    {
        var percentage = (timing.Value.TotalSeconds / totalTime) * 100;
        timingTable.AddRow(
            timing.Key,
            $"{timing.Value.TotalSeconds:F2}s",
            $"{percentage:F1}%"
        );
    }
    
    AnsiConsole.Write(timingTable);
    
    // Performance metrics panel
    var performancePanel = new Panel(new Markup($"""
        [bold yellow]Features per second:[/] [green]{totalFeatures / timings["Total Data Processing"].TotalSeconds:F0}[/]
        [bold yellow]Memory efficiency:[/] [green]{finalMemory / timings["Total Data Processing"].TotalSeconds:F2} MB/sec[/]
        [bold yellow]Batch size:[/] [green]{optimalBatchSize:N0}[/]
        [bold yellow]Thread utilization:[/] [green]{producerCount} producers, {consumerCount} consumers[/]
        """))
    {
        Header = new PanelHeader("[bold green]Performance Metrics[/]"),
        Border = BoxBorder.Rounded,
        BorderStyle = Style.Parse("green")
    };
    AnsiConsole.Write(performancePanel);
}
catch (Exception ex)
{
    AnsiConsole.WriteLine();
    var errorPanel = new Panel(new Markup($"""
        [bold red]✗ Import failed with error:[/]
        
        [red]{ex.Message.EscapeMarkup()}[/]
        
        {(ex.InnerException != null ? $"[dim]Inner error: {ex.InnerException.Message.EscapeMarkup()}[/]" : "")}
        """))
    {
        Header = new PanelHeader("[bold red]Error[/]"),
        Border = BoxBorder.Double,
        BorderStyle = Style.Parse("red bold")
    };
    AnsiConsole.Write(errorPanel);
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
