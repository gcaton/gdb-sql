# Justfile for GDB to SQL Converter

# Default recipe - show available commands
[private]
default:
    @just --list

# Build and start SQL Server container
up:
    docker-compose -f .docker/docker-compose.yml up -d

# Stop and remove containers, networks, volumes
down:
    docker-compose -f .docker/docker-compose.yml down -v
    docker rm -f gdb-sqlserver 2>/dev/null || true

# Restart SQL Server (tear down and bring up)
restart: down up

# Show SQL Server logs
logs:
    docker-compose -f .docker/docker-compose.yml logs -f sqlserver

# Build the .NET application
build:
    dotnet build ./src/gdbsql.csproj

# Run the .NET application
run:
    cd src && dotnet run

# Clean build artifacts
clean:
    dotnet clean ./src/gdbsql.csproj

# Full reset - tear down Docker and clean build
reset: down clean

# Run with fresh database - restart containers and run app
fresh: restart
    @echo "Waiting for SQL Server to be ready..."
    @sleep 10
    cd src && dotnet run

# Test SQL connection
test-connection:
    docker exec gdb-sqlserver /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'pa55w0rd!' -C \
        -Q "SELECT DB_NAME() AS CurrentDatabase; SELECT name FROM sys.databases;"

# Enter SQL Server container shell
shell:
    docker exec -it gdb-sqlserver /bin/bash

