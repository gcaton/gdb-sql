using Microsoft.Data.SqlClient;

namespace GdbToSql;

public static class ConnectionTester
{
    public static async Task<bool> TestDatabaseConnectionAsync(string connectionString)
    {
        try
        {
            Console.WriteLine($"[TEST] Testing connection: {connectionString}");
            
            using var connection = new SqlConnection(connectionString);
            Console.WriteLine($"[TEST] Opening connection...");
            
            await connection.OpenAsync();
            Console.WriteLine($"[TEST] Connection opened successfully");
            
            var cmd = new SqlCommand("SELECT 1", connection);
            var result = await cmd.ExecuteScalarAsync();
            
            Console.WriteLine($"[TEST] Test query result: {result}");
            Console.WriteLine($"[TEST] Database connection test PASSED");
            
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[TEST ERROR] Database connection test FAILED: {ex.Message}");
            Console.WriteLine($"[TEST ERROR] Stack trace: {ex.StackTrace}");
            return false;
        }
    }
}