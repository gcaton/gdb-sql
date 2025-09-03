using GdbToSql;
using Microsoft.Extensions.Configuration;
using System.Runtime.InteropServices;

try
{
    Console.WriteLine("GDB to SQL Converter");
    Console.WriteLine("====================");
    
    // Display platform and geometry format
    var isWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
    Console.WriteLine($"Platform: {RuntimeInformation.OSDescription}");
    Console.WriteLine($"Geometry format: {(isWindows ? "SQL Geography" : "WKT with SRID column")}");
    Console.WriteLine();
    
    // Load configuration
    var configuration = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional: false, reloadOnChange: false)
        .Build();
    
    var settings = configuration.Get<AppSettings>();
    
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
    
    // Read all layers from GDB
    var allLayersData = GdbReader.ReadAllGdbLayers(settings.GdbToSql.SourceGdbPath);
    
    if (allLayersData.Count == 0)
    {
        Console.WriteLine("No layers with features found in GDB");
        return;
    }
    
    Console.WriteLine($"\nFound {allLayersData.Count} layers with features to process");
    Console.WriteLine();
    
    // Insert into SQL
    var sqlWriter = new SqlWriter(settings.ConnectionStrings.DefaultConnection);
    var totalFeatures = 0;
    
    foreach (var (layerName, features) in allLayersData)
    {
        var tableName = $"{settings.GdbToSql.TargetTablePrefix}{layerName}";
        
        Console.WriteLine($"Processing layer '{layerName}' -> table '{tableName}'");
        
        await sqlWriter.CreateTableIfNotExistsAsync(tableName, features);
        
        Console.Write($"  Inserting {features.Count} features... ");
        await sqlWriter.BulkInsertAsync(tableName, features);
        Console.WriteLine("Done");
        
        totalFeatures += features.Count;
    }
    
    Console.WriteLine($"\nSuccessfully processed {allLayersData.Count} layers with {totalFeatures} total features");
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
