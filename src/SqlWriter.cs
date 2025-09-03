using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Types;
using System.Data;
using System.Data.SqlTypes;
using System.Runtime.InteropServices;
using System.Text;

namespace GdbToSql;

public class SqlWriter
{
    private readonly string _connectionString;
    private static readonly bool IsWindows = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
    
    public SqlWriter(string connectionString)
    {
        // For now, use the original connection string to avoid credential issues
        // TODO: Re-enable connection string optimization once credentials are working
        _connectionString = connectionString;
        
        // Optimize connection string for performance (disabled temporarily)
        // var builder = new SqlConnectionStringBuilder(connectionString)
        // {
        //     Pooling = true,
        //     MinPoolSize = 5,
        //     MaxPoolSize = 100,
        //     ConnectTimeout = 30
        // };
        // _connectionString = builder.ConnectionString;
    }
    
    public async Task CreateTableIfNotExistsAsync(string tableName, List<Dictionary<string, object?>> sampleData)
    {
        if (!sampleData.Any())
            return;
            
        try
        {
            using var connection = new SqlConnection(_connectionString);
            Console.WriteLine($"[SQL] Opening connection for table creation...");
            await connection.OpenAsync();
            Console.WriteLine($"[SQL] Connection opened successfully");
            
            // First check if table exists
            var checkCmd = new SqlCommand(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName", 
                connection);
            checkCmd.Parameters.AddWithValue("@tableName", tableName);
            
            Console.WriteLine($"[SQL] Checking if table '{tableName}' exists...");
            var tableExists = ((int?)await checkCmd.ExecuteScalarAsync() ?? 0) > 0;
            Console.WriteLine($"[SQL] Table exists check result: {tableExists}");
            
            if (!tableExists)
            {
                Console.WriteLine($"[SQL] Generating CREATE TABLE SQL for '{tableName}'...");
                var createTableSql = GenerateCreateTableSql(tableName, sampleData.First());
                Console.WriteLine($"[SQL] CREATE TABLE SQL: {createTableSql}");
                
                var createCmd = new SqlCommand(createTableSql, connection);
                await createCmd.ExecuteNonQueryAsync();
                Console.WriteLine($"[SQL] Table '{tableName}' created successfully");
            }
            else
            {
                Console.WriteLine($"[SQL] Table '{tableName}' already exists");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SQL ERROR] Failed to create table '{tableName}': {ex.Message}");
            Console.WriteLine($"[SQL ERROR] Stack trace: {ex.StackTrace}");
            throw;
        }
    }
    
    public async Task ProcessStreamingBatchAsync(LayerBatch batch)
    {
        try
        {
            lock (ConsoleWriteLock)
            {
                Console.WriteLine($"[SQL] Processing batch for '{batch.LayerName}' - Features: {batch.Features.Count}, FirstBatch: {batch.IsFirstBatch}, LastBatch: {batch.IsLastBatch}");
            }
            
            if (batch.Features.Count == 0 && !batch.IsLastBatch)
            {
                lock (ConsoleWriteLock)
                {
                    Console.WriteLine($"[SQL] Skipping empty batch for '{batch.LayerName}'");
                }
                return;
            }
                
            // Test database connection first
            await TestConnectionAsync();
            
            // Create table on first batch
            if (batch.IsFirstBatch && batch.Features.Count > 0)
            {
                lock (ConsoleWriteLock)
                {
                    Console.WriteLine($"[SQL] Creating table '{batch.TableName}' for first batch");
                }
                await CreateTableIfNotExistsAsync(batch.TableName, batch.Features);
            }
            
            // Insert data if there are features
            if (batch.Features.Count > 0)
            {
                lock (ConsoleWriteLock)
                {
                    Console.WriteLine($"[SQL] Inserting {batch.Features.Count} features into '{batch.TableName}'");
                }
                await BulkInsertStreamingAsync(batch.TableName, batch.Features);
                
                lock (ConsoleWriteLock)
                {
                    Console.WriteLine($"[SQL] Successfully inserted {batch.Features.Count} features into '{batch.TableName}'");
                }
            }
            
            // Log completion on last batch
            if (batch.IsLastBatch)
            {
                lock (ConsoleWriteLock)
                {
                    Console.WriteLine($"[SQL] Consumer completed layer '{batch.LayerName}' -> table '{batch.TableName}'");
                }
            }
        }
        catch (Exception ex)
        {
            lock (ConsoleWriteLock)
            {
                Console.WriteLine($"[SQL ERROR] Failed to process batch for '{batch.LayerName}': {ex.Message}");
                Console.WriteLine($"[SQL ERROR] Stack trace: {ex.StackTrace}");
            }
            throw;
        }
    }
    
    private async Task TestConnectionAsync()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            lock (ConsoleWriteLock)
            {
                Console.WriteLine($"[SQL] Database connection test successful");
            }
        }
        catch (Exception ex)
        {
            lock (ConsoleWriteLock)
            {
                Console.WriteLine($"[SQL ERROR] Database connection failed: {ex.Message}");
            }
            throw;
        }
    }
    
    private static readonly object ConsoleWriteLock = new();
    
    private async Task BulkInsertStreamingAsync(string tableName, List<Dictionary<string, object?>> data)
    {
        try
        {
            Console.WriteLine($"[SQL] Starting bulk insert of {data.Count} records to '{tableName}'");
            
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            
            // Create DataTable for bulk copy
            var dataTable = new DataTable();
            var firstRow = data.First();
            
            Console.WriteLine($"[SQL] Creating DataTable with {firstRow.Keys.Count} columns");
            
            // Add columns
            foreach (var kvp in firstRow)
            {
                if (kvp.Key == "GEOMETRY" && IsWindows)
                {
                    dataTable.Columns.Add(kvp.Key, typeof(SqlGeography));
                }
                else if (kvp.Key == "WKT_GEOMETRY" && !IsWindows)
                {
                    dataTable.Columns.Add(kvp.Key, typeof(string));
                }
                else if (kvp.Key == "SRID" && !IsWindows)
                {
                    dataTable.Columns.Add(kvp.Key, typeof(int));
                }
                else
                {
                    var columnType = GetSqlDataType(kvp.Value);
                    dataTable.Columns.Add(kvp.Key, columnType);
                }
            }
            
            Console.WriteLine($"[SQL] Adding {data.Count} rows to DataTable");
            
            // Add rows
            foreach (var row in data)
            {
                var dataRow = dataTable.NewRow();
                foreach (var kvp in row)
                {
                    if (kvp.Key == "GEOMETRY" && kvp.Value is SqlGeography geog && IsWindows)
                    {
                        dataRow[kvp.Key] = geog;
                    }
                    else
                    {
                        dataRow[kvp.Key] = kvp.Value ?? DBNull.Value;
                    }
                }
                dataTable.Rows.Add(dataRow);
            }
            
            Console.WriteLine($"[SQL] Performing bulk copy to '{tableName}'");
            
            // Perform bulk copy with optimizations
            using var bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.DestinationTableName = tableName;
            bulkCopy.BulkCopyTimeout = 300;
            bulkCopy.BatchSize = Math.Min(data.Count, 10000);
            bulkCopy.EnableStreaming = true;
            bulkCopy.NotifyAfter = Math.Min(data.Count / 4, 2500);
            
            await bulkCopy.WriteToServerAsync(dataTable);
            
            Console.WriteLine($"[SQL] Bulk copy completed successfully for '{tableName}'");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[SQL ERROR] Bulk insert failed for '{tableName}': {ex.Message}");
            Console.WriteLine($"[SQL ERROR] Stack trace: {ex.StackTrace}");
            throw;
        }
    }

    public async Task BulkInsertAsync(string tableName, List<Dictionary<string, object?>> data)
    {
        if (!data.Any())
            return;
            
        const int batchSize = 10000; // Insert in batches of 10k records
        var totalBatches = (int)Math.Ceiling(data.Count / (double)batchSize);
        
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        // Create DataTable schema from first row
        var schemaTable = new DataTable();
        var firstRow = data.First();
        
        // Add columns
        foreach (var kvp in firstRow)
        {
            if (kvp.Key == "GEOMETRY" && IsWindows)
            {
                // Special handling for geography column on Windows
                schemaTable.Columns.Add(kvp.Key, typeof(SqlGeography));
            }
            else if (kvp.Key == "WKT_GEOMETRY" && !IsWindows)
            {
                // WKT string on Linux
                schemaTable.Columns.Add(kvp.Key, typeof(string));
            }
            else if (kvp.Key == "SRID" && !IsWindows)
            {
                // SRID integer on Linux
                schemaTable.Columns.Add(kvp.Key, typeof(int));
            }
            else
            {
                var columnType = GetSqlDataType(kvp.Value);
                schemaTable.Columns.Add(kvp.Key, columnType);
            }
        }
        
        // Process data in batches
        for (int batchNum = 0; batchNum < totalBatches; batchNum++)
        {
            var batchData = data.Skip(batchNum * batchSize).Take(batchSize).ToList();
            var dataTable = schemaTable.Clone(); // Clone schema
            
            // Add rows for this batch
            foreach (var row in batchData)
            {
                var dataRow = dataTable.NewRow();
                foreach (var kvp in row)
                {
                    if (kvp.Key == "GEOMETRY" && kvp.Value is SqlGeography geog && IsWindows)
                    {
                        dataRow[kvp.Key] = geog;
                    }
                    else
                    {
                        dataRow[kvp.Key] = kvp.Value ?? DBNull.Value;
                    }
                }
                dataTable.Rows.Add(dataRow);
            }
            
            // Perform bulk copy for this batch
            using var bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.DestinationTableName = tableName;
            bulkCopy.BulkCopyTimeout = 300; // 5 minutes per batch
            bulkCopy.BatchSize = batchSize;
            
            await bulkCopy.WriteToServerAsync(dataTable);
            
            // Show progress (thread-safe)
            if (totalBatches > 1)
            {
                var progress = ((batchNum + 1) * 100) / totalBatches;
                // Don't show inline progress when running in parallel to avoid console conflicts
                // The main thread will show overall progress
            }
        }
    }
    
    private string GenerateCreateTableSql(string tableName, Dictionary<string, object?> sampleRow)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE [{tableName}] (");
        
        var columns = new List<string>();
        foreach (var kvp in sampleRow)
        {
            var sqlType = GetSqlColumnType(kvp.Key, kvp.Value);
            columns.Add($"[{kvp.Key}] {sqlType}");
        }
        
        sb.AppendLine(string.Join(",\n", columns));
        sb.AppendLine(")");
        
        return sb.ToString();
    }
    
    private Type GetSqlDataType(object? value)
    {
        if (value == null)
            return typeof(string);
            
        return value.GetType();
    }
    
    private string GetSqlColumnType(string columnName, object? sampleValue)
    {
        if (columnName == "GEOMETRY" && IsWindows)
            return "GEOGRAPHY";
            
        if (columnName == "WKT_GEOMETRY" && !IsWindows)
            return "NVARCHAR(MAX)";
            
        if (columnName == "SRID" && !IsWindows)
            return "INT";
            
        if (columnName == "FID")
            return "BIGINT";
            
        if (sampleValue == null)
            return "NVARCHAR(255)";
            
        return sampleValue switch
        {
            int _ => "INT",
            long _ => "BIGINT",
            double _ => "FLOAT",
            float _ => "REAL",
            decimal _ => "DECIMAL(18,6)",
            DateTime _ => "DATETIME2",
            bool _ => "BIT",
            SqlGeography _ => "GEOGRAPHY",
            _ => "NVARCHAR(MAX)"
        };
    }
}