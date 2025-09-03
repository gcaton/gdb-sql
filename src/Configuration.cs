namespace GdbToSql;

public class AppSettings
{
    public ConnectionStringsSection ConnectionStrings { get; set; } = new();
    public GdbToSqlSection GdbToSql { get; set; } = new();
}

public class ConnectionStringsSection
{
    public string DefaultConnection { get; set; } = string.Empty;
}

public class GdbToSqlSection
{
    public string SourceGdbPath { get; set; } = string.Empty;
    public string TargetTablePrefix { get; set; } = "GDB_";
}