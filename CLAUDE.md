# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

GDB to SQL Converter - A high-performance .NET 9 console application that converts Esri Geodatabase (GDB) files to SQL Server using GDAL, featuring multi-threaded processing with a producer-consumer pattern for optimal performance.

## Development Commands

```bash
# Build the project
dotnet build

# Run the application
dotnet run --project src/

# Restore NuGet packages
dotnet restore

# Clean build artifacts
dotnet clean
```

**IMPORTANT**: After making any code changes, always run `dotnet build` in the src directory to check for compilation errors before proceeding.

## Architecture

### Core Components

- **Program.cs**: Main entry point that orchestrates the conversion process using producer-consumer pattern
- **GdbReader.cs**: Uses GDAL/OGR to read features from GDB files with 16 parallel producer threads
- **SqlWriter.cs**: Handles SQL Server table creation and bulk insertion with 4 consumer threads
- **StreamingModels.cs**: Data models for feature streaming between readers and writers
- **Configuration.cs**: Strongly-typed configuration models
- **TestConnection.cs**: Database connection testing and validation
- **appsettings.json**: Configuration for database connection, GDB paths, and processing options

### Processing Flow

1. **Initialization**: Validates configuration, tests SQL connection, initializes GDAL
2. **Schema Discovery**: Reads GDB structure to determine table schema
3. **Table Creation**: Creates SQL tables with appropriate data types and geometry columns
4. **Parallel Processing**: 
   - 16 producer threads read features from GDB
   - Features flow through .NET Channels
   - 4 consumer threads batch insert to SQL Server
5. **Performance Monitoring**: Tracks records/second and MB/s throughput

### Key Design Patterns

- **Producer-Consumer**: Separates reading and writing for optimal performance
- **Channel-based Communication**: Thread-safe data flow between components
- **Batch Processing**: Configurable batch sizes (default 100, increases for large datasets)
- **Connection Pooling**: Reuses SQL connections across threads

### Platform-Specific Behavior

- **Windows**: Uses SQL Server Geography type directly
- **Linux**: Stores geometry as WKT (Well-Known Text) with separate SRID column

## Configuration

Edit `src/appsettings.json`:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=localhost;Database=YourDB;Integrated Security=true;TrustServerCertificate=true"
  },
  "GdbToSql": {
    "SourceGdbPath": "path/to/your.gdb",
    "TargetTablePrefix": "imported_"
  }
}
```

## Data Type Mapping

The application automatically maps GDB types to SQL Server:
- OBJECTID → INT (Primary Key)
- OFTString → NVARCHAR (length auto-detected)
- OFTInteger/OFTInteger64 → INT/BIGINT
- OFTReal → FLOAT
- OFTDate/OFTDateTime → DATETIME2
- OFTBinary → VARBINARY(MAX)
- Geometry → GEOGRAPHY or NVARCHAR(MAX) + INT (platform-dependent)

## Performance Considerations

- Default SRID: 4283 (GDA94)
- Batch size dynamically adjusts based on record count
- SqlBulkCopy optimized with table lock and streaming
- Memory usage monitored to prevent excessive consumption
- Typical performance: ~29,000 features/second, ~145 MB/s