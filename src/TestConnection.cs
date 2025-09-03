using Microsoft.Data.SqlClient;
using Spectre.Console;

namespace GdbToSql;

public static class ConnectionTester
{
    public static async Task<bool> TestDatabaseConnectionAsync(string connectionString)
    {
        try
        {
            // Testing connection silently
            
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            var cmd = new SqlCommand("SELECT 1", connection);
            var result = await cmd.ExecuteScalarAsync();
            
            // Connection test passed
            
            return true;
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine($"[red]✗ Database connection test failed: {ex.Message.EscapeMarkup()}[/]");
            return false;
        }
    }
}