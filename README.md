# GDB to SQL Converter

A .NET console application that reads all records from Esri Geodatabase (GDB) files and imports them into SQL Server with intelligent data type detection, geometry processing, and multi-threaded performance optimisation.

## 🚀 Features

### **Core Functionality**
- **Complete GDB Import** - Reads all feature classes from GDB folders automatically
- **Geometry Processing** - Handles spatial data with coordinate system support (SRID 4283 - GDA94)
- **Cross-Platform** - Works on Windows (SQL Geography) and Linux (WKT format)

## 📋 Requirements

- **.NET 9 Runtime**
- **SQL Server** (2016+) or **SQL Server Express** (or use provided Docker setup)
- **Geodatabase Files** (.gdb format)
- **Docker** (optional, for containerized SQL Server)
- **just** command runner (optional, for simplified commands)

## ⚙️ Configuration

Configure the application by editing `appsettings.json`:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=localhost;Database=gdb;User Id=sa;Password=pa55w0rd!;TrustServerCertificate=True"
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

### **Quick Start with Docker**
```bash
# Start SQL Server container
just up

# Run the application
just run

# Or do a fresh run (restart DB and run app)
just fresh
```

### **Available Commands (via justfile)**
```bash
just up          # Start SQL Server container
just down        # Stop and remove containers
just restart     # Restart containers
just build       # Build the .NET application
just run         # Run the application
just fresh       # Restart DB and run app
just logs        # View SQL Server logs
just status      # Check container status
just test-connection  # Test SQL connection
```

### **Manual Usage**
```bash
# Without justfile
cd src && dotnet run

# Or with docker-compose directly
docker-compose -f .docker/docker-compose.yml up -d
cd src && dotnet run
```

### **Sample Output**
The application provides a clean, informative console output showing:
- System information and configuration
- Layer discovery with feature counts
- Real-time progress updates
- Performance metrics and timing summary

**Note**: Table creation and deletion operations are performed silently for cleaner output.

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

## 🐳 Docker Setup

The project includes a Docker setup for SQL Server:

### **Docker Files**
- `.docker/Dockerfile.sqlserver` - SQL Server 2022 container with auto-initialization
- `.docker/docker-compose.yml` - Docker Compose configuration
- `.docker/init-db.sql` - Database initialization script (creates `gdb` database)

### **Default Credentials**
- **Database**: `gdb`
- **Username**: `sa`
- **Password**: `pa55w0rd!`
- **Port**: `1433`

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