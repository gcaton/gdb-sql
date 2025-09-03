# GDB to SQL Converter

A .NET console application that reads all records from Esri Geodatabase (GDB) files and imports them into SQL Server with intelligent data type detection, geometry processing, and multi-threaded performance optimisation.

## 🚀 Features

### **Core Functionality**
- **Complete GDB Import** - Reads all feature classes from GDB folders automatically
- **Intelligent Table Creation** - Creates SQL Server tables with proper data types
- **Geometry Processing** - Handles spatial data with coordinate system support (SRID 4283 - GDA94)
- **Cross-Platform** - Works on Windows (SQL Geography) and Linux (WKT format)

## 📋 Requirements

- **.NET 9 Runtime**
- **SQL Server** (2016+) or **SQL Server Express**
- **Geodatabase Files** (.gdb format)

## ⚙️ Configuration

Configure the application by editing `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=localhost,1435;Database=master;User Id=sa;Password=YourPassword;TrustServerCertificate=True"
  },
  "GdbToSql": {
    "SourceGdbPath": "NSW.gdb",
    "TargetTablePrefix": "NSW_"
  }
}
```

### **Configuration Options:**
- **DefaultConnection**: SQL Server connection string
- **SourceGdbPath**: Path to your .gdb folder
- **TargetTablePrefix**: Prefix for created SQL tables (e.g., "NSW_" creates "NSW_Roads", "NSW_Properties")

## 🏃‍♂️ Usage

### **Basic Usage**
```bash
dotnet run
```

### **Build and Run**
```bash
dotnet build
dotnet run
```

### **Sample Output**
```
GDB to SQL Converter
====================
Platform: Linux
Geometry format: WKT with SRID column

Reading from GDB: NSW.gdb
Table prefix: NSW_

Database connection successful.
Found 33 layers with 10,923,521 total features to process
Using dynamic batch size: 10,000 features per batch
Using 16 producer threads and 4 consumer threads

==================================================
PERFORMANCE TIMING SUMMARY
==================================================
Configuration Load          : 0.02s
Database Connection Test     : 0.15s  
Layer Information Scan       : 2.45s
Producer Processing          : 245.67s
Consumer Processing          : 123.89s
Total Data Processing        : 369.56s
Total Application Time       : 372.18s
==================================================
Features per second          : 29,562
MB processed per second      : 145.23
==================================================
```

## 🗃️ Data Types

The application intelligently detects and converts data types:

| **Source Data** | **SQL Server Type** | **Detection Logic** |
|-----------------|---------------------|-------------------|
| OBJECTID | `INT` | Column name pattern |
| FID | `BIGINT` | Always long integer |
| Date fields | `DATETIME2` | Name contains "date"/"time" |
| Boolean fields | `BIT` | Name starts with "is"/"has" |
| ID fields | `INT` | Name ends with "id" + numeric |
| Short text | `NVARCHAR(50)` | Length ≤ 50 characters |
| Medium text | `NVARCHAR(255)` | Length ≤ 255 characters |
| Long text | `NVARCHAR(MAX)` | Length > 1000 characters |
| Geometry | `GEOGRAPHY` (Windows) | Spatial data with SRID 4283 |
| Geometry | `NVARCHAR(MAX) + SRID` (Linux) | WKT format with SRID column |

## 🌍 Spatial Data Support

### **Coordinate System**
- **SRID 4283** - GDA94 (Geocentric Datum of Australia 1994)
- **Anti-clockwise orientation** - Ensures OGC compliance for polygons

### **Platform Differences**
- **Windows**: Uses SQL Server `GEOGRAPHY` data type
- **Linux**: Uses WKT strings with separate SRID column

## 🛠️ Development

### **Key Architecture**
- **Producer-Consumer Pattern** - Separates reading and writing for optimal performance  
- **Channel-based Communication** - .NET Channels for thread-safe data flow
- **Batch Processing** - Configurable batch sizes for memory efficiency
- **Connection Pooling** - Reuses database connections across threads

### **Performance Tuning**
The application includes several performance optimizations:
- Dynamic batch sizing based on data volume
- Parallel geometry processing
- Optimized SqlBulkCopy settings
- Memory usage monitoring

### **Error Handling**
- Graceful recovery from geometry conversion errors
- Empty string handling for numeric columns
- Detailed error logging with stack traces
- Transaction rollback on failures

## 🆘 Troubleshooting

**Geometry Errors:**
```bash
Warning: Failed to convert geometry for feature
```
- Check coordinate system compatibility
- Verify GDB file integrity
- Review geometry validation logs