# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GDB to SQL Converter - A .NET 9 console application that reads geodatabase (GDB) files and inserts records into SQL Server tables using the GDAL library.

## Development Commands

```bash
# Build the project
dotnet build

# Run the application
dotnet run --project src/

# Restore NuGet packages
dotnet restore
```

**IMPORTANT**: After making any code changes, always run `dotnet build` in the src directory to check for compilation errors before proceeding.

## Architecture

The application consists of:
- **Program.cs**: Main entry point that orchestrates the conversion process
- **GdbReader.cs**: Uses GDAL/OGR to read features from GDB files
- **SqlWriter.cs**: Handles SQL Server table creation and bulk insertion
- **Configuration.cs**: Models for strongly-typed configuration
- **appsettings.json**: Configuration file for database connection and GDB settings

Key dependencies:
- GDAL.NET & GDAL: For reading geodatabase files
- Microsoft.Data.SqlClient: For SQL Server connectivity
- Microsoft.Extensions.Configuration: For configuration management

## Configuration

Edit `src/appsettings.json` to configure:
- Connection string for SQL Server
- Source GDB file path
- Target SQL table name
- Feature class name to read from GDB