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
        _connectionString = connectionString;
    }
    
    public async Task CreateTableIfNotExistsAsync(string tableName, List<Dictionary<string, object?>> sampleData)
    {
        if (!sampleData.Any())
            return;
            
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        // First check if table exists
        var checkCmd = new SqlCommand(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName", 
            connection);
        checkCmd.Parameters.AddWithValue("@tableName", tableName);
        
        var tableExists = ((int?)await checkCmd.ExecuteScalarAsync() ?? 0) > 0;
        
        if (!tableExists)
        {
            var createTableSql = GenerateCreateTableSql(tableName, sampleData.First());
            var createCmd = new SqlCommand(createTableSql, connection);
            await createCmd.ExecuteNonQueryAsync();
            Console.WriteLine($"    Table '{tableName}' created");
        }
        else
        {
            Console.WriteLine($"    Table '{tableName}' already exists");
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
            
            // Show progress
            if (totalBatches > 1)
            {
                var progress = ((batchNum + 1) * 100) / totalBatches;
                Console.Write($"\r  Inserting {data.Count} features... {progress}%");
            }
        }
        
        if (totalBatches > 1)
        {
            Console.Write("\r"); // Clear progress line
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