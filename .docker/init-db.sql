-- Create the gdb database
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'gdb')
BEGIN
    CREATE DATABASE gdb;
END
GO

USE gdb;
GO

-- Enable spatial data support
EXEC sp_configure 'clr enabled', 1;
RECONFIGURE;
GO

PRINT 'Database gdb initialized successfully';
GO