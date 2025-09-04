using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Types;
using System.Data;
using System.Data.SqlTypes;
using System.Runtime.InteropServices;
using System.Text;
using Spectre.Console;

namespace GdbToSql;

public class SqlWriter
{
    private readonly string _connectionString;
    
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
            await connection.OpenAsync();
            
            // First check if table exists
            var checkCmd = new SqlCommand(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName", 
                connection);
            checkCmd.Parameters.AddWithValue("@tableName", tableName);
            
            var tableExists = ((int?)await checkCmd.ExecuteScalarAsync() ?? 0) > 0;
            
            if (tableExists)
            {
                AnsiConsole.MarkupLine($"[yellow]⚠ Dropping existing table [bold]{tableName}[/] for clean data...[/]");
                var dropCmd = new SqlCommand($"DROP TABLE [{tableName}]", connection);
                await dropCmd.ExecuteNonQueryAsync();
            }
            
            var createTableSql = GenerateCreateTableSql(tableName, sampleData);
            
            var createCmd = new SqlCommand(createTableSql, connection);
            await createCmd.ExecuteNonQueryAsync();
            AnsiConsole.MarkupLine($"[green]✓ Created table [bold]{tableName}[/][/]");
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[bold red]✗ Failed to create table [bold]{tableName}[/]: {ex.Message.EscapeMarkup()}[/]");
            throw;
        }
    }
    
    public async Task ProcessStreamingBatchAsync(LayerBatch batch)
    {
        try
        {
            // Processing batch silently for cleaner output
            
            if (batch.Features.Count == 0 && !batch.IsLastBatch)
            {
                return; // Skip empty batch silently
            }
                
            // Test database connection first
            await TestConnectionAsync();
            
            // Create table on first batch
            if (batch.IsFirstBatch && batch.Features.Count > 0)
            {
                // Create table for first batch
                await CreateTableIfNotExistsAsync(batch.TableName, batch.Features);
            }
            
            // Insert data if there are features
            if (batch.Features.Count > 0)
            {
                // Insert features silently
                await BulkInsertStreamingAsync(batch.TableName, batch.Features);
                
                // Batch inserted successfully
            }
            
            // Log completion on last batch
            if (batch.IsLastBatch)
            {
                // Layer processing completed
            }
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]✗ Batch failed for [bold]{batch.LayerName}[/]: {ex.Message.EscapeMarkup()}[/]");
            throw;
        }
    }
    
    private async Task TestConnectionAsync()
    {
        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            // Connection test successful (logged elsewhere)
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]✗ Database connection failed: {ex.Message.EscapeMarkup()}[/]");
            throw;
        }
    }
    
    // Console write lock removed - no longer needed with Spectre.Console
    
    private async Task BulkInsertStreamingAsync(string tableName, List<Dictionary<string, object?>> data)
    {
        try
        {
            // Starting bulk insert silently
            
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            
            // Create DataTable for bulk copy
            var dataTable = new DataTable();
            var firstRow = data.First();
            
            // Creating DataTable silently
            
            // Add columns with proper data types
            foreach (var kvp in firstRow)
            {
                if (kvp.Key == "SHAPE")
                {
                    dataTable.Columns.Add(kvp.Key, typeof(Object));
                }
                else
                {
                    var columnType = GetOptimizedDataType(kvp.Key, kvp.Value);
                    dataTable.Columns.Add(kvp.Key, columnType);
                }
            }
            
            // Adding rows to DataTable silently
            
            // Add rows with type conversion
            foreach (var row in data)
            {
                var dataRow = dataTable.NewRow();
                foreach (var kvp in row)
                {
                    if (kvp.Key == "SHAPE")
                    {
                        dataRow[kvp.Key] = kvp.Value;
                    }
                    else if (kvp.Value == null)
                    {
                        dataRow[kvp.Key] = DBNull.Value;
                    }
                    else
                    {
                        // Convert to appropriate data type
                        dataRow[kvp.Key] = ConvertToAppropriateType(kvp.Key, kvp.Value, dataTable.Columns[kvp.Key]?.DataType ?? typeof(string));
                    }
                }
                dataTable.Rows.Add(dataRow);
            }
            
            // Performing bulk copy silently
            
            // Perform bulk copy with optimizations
            using var bulkCopy = new SqlBulkCopy(connection);
            bulkCopy.DestinationTableName = tableName;
            bulkCopy.BulkCopyTimeout = 300;
            bulkCopy.BatchSize = Math.Min(data.Count, 10000);
            bulkCopy.EnableStreaming = true;
            bulkCopy.NotifyAfter = Math.Min(data.Count / 4, 2500);
            
            await bulkCopy.WriteToServerAsync(dataTable);
            
            // Bulk copy completed silently
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]✗ Bulk insert failed for [bold]{tableName}[/]: {ex.Message.EscapeMarkup()}[/]");
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
            if (kvp.Key == "SHAPE")
            {
                schemaTable.Columns.Add(kvp.Key, typeof(Object));
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
                    if (kvp.Key == "SHAPE")
                    {
                        dataRow[kvp.Key] = kvp.Value;
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
    
    private string GenerateCreateTableSql(string tableName, List<Dictionary<string, object?>> allSampleData)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE [{tableName}] (");
        
        var columns = new List<string>();
        var firstRow = allSampleData.First();
        
        foreach (var kvp in firstRow)
        {
            // Analyze all sample data for this column to determine optimal type
            var sqlType = GetOptimalSqlColumnType(kvp.Key, allSampleData.Select(row => row.GetValueOrDefault(kvp.Key)));
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
        
        return value switch
        {
            int _ => typeof(int),
            long _ => typeof(long),
            double _ => typeof(double),
            float _ => typeof(float),
            decimal _ => typeof(decimal),
            DateTime _ => typeof(DateTime),
            bool _ => typeof(bool),
            SqlGeography _ => typeof(SqlGeography),
            string str when DateTime.TryParse(str, out _) => typeof(DateTime),
            string str when bool.TryParse(str, out _) => typeof(bool),
            string str when int.TryParse(str, out _) => typeof(int),
            string str when double.TryParse(str, out _) => typeof(double),
            _ => typeof(string)
        };
    }
    
    private string GetSqlColumnType(string columnName, object? sampleValue)
    {
        if (columnName == "SHAPE")
            return "GEOGRAPHY";
            
        if (columnName == "FID")
            return "BIGINT";
            
        // Handle OBJECTID columns
        if (columnName.ToUpper() == "OBJECTID" || columnName.ToLower().Contains("objectid"))
            return "INT";
            
        // Handle ID columns - be more conservative
        if ((columnName.ToLower().EndsWith("id") || columnName.ToLower() == "objectid") && 
            sampleValue is string idStr && !string.IsNullOrWhiteSpace(idStr) && int.TryParse(idStr, out _))
            return "INT";
            
        // Date and boolean detection temporarily disabled for maximum compatibility
        // All date/boolean columns will be stored as NVARCHAR to prevent conversion errors
            
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
            string str when str.Length <= 50 => "NVARCHAR(50)",
            string str when str.Length <= 255 => "NVARCHAR(255)",
            string str when str.Length <= 1000 => "NVARCHAR(1000)",
            string _ => "NVARCHAR(MAX)",
            _ => "NVARCHAR(MAX)"
        };
    }
    
    private Type GetOptimizedDataType(string columnName, object? value)
    {
        if (value == null)
            return typeof(string);
            
        // Handle special columns - be more conservative
        if (columnName.ToUpper() == "OBJECTID")
            return typeof(int);
            
        if (columnName == "FID")
            return typeof(long);
            
        // Date and boolean detection temporarily disabled for maximum compatibility
        // All date/boolean columns will be stored as NVARCHAR to prevent conversion errors
        
        return value switch
        {
            int _ => typeof(int),
            long _ => typeof(long),
            double _ => typeof(double),
            float _ => typeof(float),
            decimal _ => typeof(decimal),
            DateTime _ => typeof(DateTime),
            bool _ => typeof(bool),
            SqlGeography _ => typeof(SqlGeography),
            // Date and boolean detection removed for compatibility
            // Be more conservative with integer detection - only for columns clearly meant to be integers
            string str when !string.IsNullOrWhiteSpace(str) && int.TryParse(str, out _) && 
                           (columnName.ToLower().EndsWith("id") || columnName.ToLower() == "objectid") => typeof(int),
            string str when !string.IsNullOrWhiteSpace(str) && double.TryParse(str, out _) && 
                           (columnName.ToLower().Contains("factor") || columnName.ToLower().Contains("scale")) => typeof(double),
            _ => typeof(string)
        };
    }
    
    private object ConvertToAppropriateType(string columnName, object value, Type targetType)
    {
        if (value == null)
            return DBNull.Value;
            
        // Handle empty strings - convert to null for non-string types
        if (value is string str && string.IsNullOrWhiteSpace(str) && targetType != typeof(string))
            return DBNull.Value;
            
        if (targetType == typeof(DateTime) && value is string dateStr)
        {
            if (DateTime.TryParse(dateStr, out DateTime dateResult))
                return dateResult;
            return DBNull.Value; // Invalid date becomes null
        }
        
        if (targetType == typeof(bool) && value is string boolStr)
        {
            if (bool.TryParse(boolStr, out bool boolResult))
                return boolResult;
            // Handle common boolean representations
            return boolStr.ToLower() switch
            {
                "1" or "yes" or "y" or "true" or "t" => true,
                "0" or "no" or "n" or "false" or "f" => false,
                "undefined" or "null" or "" => DBNull.Value, // Handle undefined/null values
                _ => DBNull.Value // Invalid boolean becomes null
            };
        }
        
        if (targetType == typeof(int) && value is string intStr)
        {
            if (int.TryParse(intStr, out int intResult))
                return intResult;
            return DBNull.Value; // Invalid integer becomes null
        }
        
        if (targetType == typeof(long) && value is string longStr)
        {
            if (long.TryParse(longStr, out long longResult))
                return longResult;
            return DBNull.Value; // Invalid long becomes null
        }
        
        if (targetType == typeof(double) && value is string doubleStr)
        {
            if (double.TryParse(doubleStr, out double doubleResult))
                return doubleResult;
            return DBNull.Value; // Invalid double becomes null
        }
        
        if (targetType == typeof(decimal) && value is string decimalStr)
        {
            if (decimal.TryParse(decimalStr, out decimal decimalResult))
                return decimalResult;
            return DBNull.Value; // Invalid decimal becomes null
        }
        
        // If no conversion needed or possible, return original value
        return value;
    }
    
    private string GetOptimalSqlColumnType(string columnName, IEnumerable<object?> allValues)
    {
        // Handle special geometry columns
        if (columnName == "SHAPE")
            return "GEOGRAPHY";
            
        if (columnName == "FID")
            return "BIGINT";
            
        // OBJECTID disabled - too many GDB files have empty/null OBJECTID values
        // if (columnName.ToUpper() == "OBJECTID")
        //     return "INT";
            
        var nonNullValues = allValues.Where(v => v != null && !(v is string s && string.IsNullOrWhiteSpace(s))).ToList();
        
        if (!nonNullValues.Any())
            return "NVARCHAR(255)";
            
        // Check if all values can be parsed as specific types
        var allStrings = nonNullValues.OfType<string>().ToList();
        
        // Date detection - temporarily disabled to prevent conversion errors
        // Real-world GIS data has too many mixed date formats and empty values
        // Store all date-like columns as NVARCHAR for maximum compatibility
        // TODO: Re-enable with more sophisticated date validation in future version
        /*
        if ((columnName.ToLower().Contains("date") || columnName.ToLower().Contains("time")) && allStrings.Any())
        {
            var validDates = allStrings.Where(s => !string.IsNullOrWhiteSpace(s) && 
                                                  DateTime.TryParse(s, out DateTime date) && 
                                                  date != DateTime.MinValue && 
                                                  date.Year > 1900 && date.Year < 2100).ToList();
            
            // Only treat as datetime if 95% of non-empty values are valid dates AND at least 50% of all values are non-empty
            var nonEmptyCount = allStrings.Where(s => !string.IsNullOrWhiteSpace(s)).Count();
            if (validDates.Count > nonEmptyCount * 0.95 && nonEmptyCount > allStrings.Count * 0.5)
                return "DATETIME2";
        }
        */
        
        // Boolean detection - temporarily disabled to prevent conversion errors
        // Real-world GIS data has too many mixed boolean representations and empty values
        // Store all boolean-like columns as NVARCHAR for maximum compatibility
        // TODO: Re-enable with more sophisticated boolean validation in future version
        /*
        if ((columnName.ToLower().StartsWith("is") || columnName.ToLower().StartsWith("has") || 
            columnName.ToLower() == "iscurrent") && allStrings.Any())
        {
            var validBooleans = allStrings.Where(s => 
            {
                if (string.IsNullOrWhiteSpace(s)) return false;
                var lower = s.ToLower();
                return lower is "true" or "false" or "1" or "0" or "yes" or "no" or "y" or "n" or "t" or "f";
            }).ToList();
            
            var nonEmptyCount = allStrings.Where(s => !string.IsNullOrWhiteSpace(s)).Count();
            if (validBooleans.Count > nonEmptyCount * 0.95 && nonEmptyCount > allStrings.Count * 0.5)
                return "BIT";
        }
        */
        
        // Integer detection - temporarily disabled to prevent conversion errors
        // Real-world GIS data has too many mixed integer representations and empty values
        // Store all integer-like columns as NVARCHAR for maximum compatibility
        // TODO: Re-enable with more sophisticated integer validation in future version
        /*
        if ((columnName.ToLower().EndsWith("id") || columnName.ToLower() == "objectid") && allStrings.Any())
        {
            var validInts = allStrings.Where(s => !string.IsNullOrWhiteSpace(s) && int.TryParse(s, out _)).ToList();
            
            var nonEmptyCount = allStrings.Where(s => !string.IsNullOrWhiteSpace(s)).Count();
            if (validInts.Count > nonEmptyCount * 0.95 && nonEmptyCount > allStrings.Count * 0.5)
                return "INT";
        }
        */
        
        // For string columns, find the maximum length and use appropriate NVARCHAR size
        var maxLength = 0;
        foreach (var value in allValues)
        {
            if (value is string str && !string.IsNullOrEmpty(str))
            {
                maxLength = Math.Max(maxLength, str.Length);
            }
        }
        
        // Use appropriate NVARCHAR size with some buffer
        return maxLength switch
        {
            0 => "NVARCHAR(255)",
            <= 50 => "NVARCHAR(100)",      // 100% buffer for short strings
            <= 100 => "NVARCHAR(200)",     // 100% buffer
            <= 255 => "NVARCHAR(500)",     // ~100% buffer
            <= 500 => "NVARCHAR(1000)",    // 100% buffer
            <= 1000 => "NVARCHAR(2000)",   // 100% buffer
            <= 2000 => "NVARCHAR(4000)",   // 100% buffer
            _ => "NVARCHAR(MAX)"           // For very long strings
        };
    }
}