#!/bin/bash
# Helper script to compile and run TestDataLoader

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Test Data Loader - Setup and Run"
echo "=========================================="
echo

# Check if Java is installed
if ! command -v javac &> /dev/null; then
    echo -e "${RED}Error: javac not found. Please install JDK.${NC}"
    exit 1
fi

# Compile the Java program
echo -e "${YELLOW}Compiling TestDataLoader.java...${NC}"
cd "$(dirname "$0")"
javac TestDataLoader.java

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Compilation successful${NC}"
else
    echo -e "${RED}❌ Compilation failed${NC}"
    exit 1
fi

echo
echo "=========================================="
echo "Usage Examples:"
echo "=========================================="
echo
echo "SQL Server:"
echo "  java -cp \".:mssql-jdbc-12.2.0.jre11.jar\" TestDataLoader sqlserver \\"
echo "    ./test_data \\"
echo "    \"jdbc:sqlserver://localhost:1433;databaseName=testdb;encrypt=false\" \\"
echo "    sa YourPassword dbo"
echo
echo "Oracle:"
echo "  java -cp \".:ojdbc8.jar\" TestDataLoader oracle \\"
echo "    ./test_data \\"
echo "    \"jdbc:oracle:thin:@localhost:1521:ORCL\" \\"
echo "    hr hr HR"
echo
echo "PostgreSQL:"
echo "  java -cp \".:postgresql-42.6.0.jar\" TestDataLoader postgresql \\"
echo "    ./test_data \\"
echo "    \"jdbc:postgresql://localhost:5432/testdb\" \\"
echo "    postgres postgres public"
echo
echo "DB2:"
echo "  java -cp \".:db2jcc4.jar\" TestDataLoader db2 \\"
echo "    ./test_data \\"
echo "    \"jdbc:db2://localhost:50000/testdb\" \\"
echo "    db2inst1 password DB2INST1"
echo
echo "=========================================="
echo

# Check if arguments provided
if [ $# -ge 5 ]; then
    echo -e "${YELLOW}Running TestDataLoader with provided arguments...${NC}"
    echo
    
    ENGINE=$1
    CSV_DIR=$2
    JDBC_URL=$3
    USERNAME=$4
    PASSWORD=$5
    SCHEMA=${6:-""}
    
    # Find JDBC driver
    DRIVER=""
    case $ENGINE in
        sqlserver)
            DRIVER=$(find . -name "mssql-jdbc*.jar" 2>/dev/null | head -1)
            ;;
        oracle)
            DRIVER=$(find . -name "ojdbc*.jar" 2>/dev/null | head -1)
            ;;
        postgresql)
            DRIVER=$(find . -name "postgresql*.jar" 2>/dev/null | head -1)
            ;;
        db2)
            DRIVER=$(find . -name "db2jcc*.jar" 2>/dev/null | head -1)
            ;;
    esac
    
    if [ -z "$DRIVER" ]; then
        echo -e "${RED}Warning: JDBC driver not found for $ENGINE${NC}"
        echo "Please download the appropriate JDBC driver and place it in this directory"
        echo
        CLASSPATH="."
    else
        echo -e "${GREEN}Found JDBC driver: $DRIVER${NC}"
        CLASSPATH=".:$DRIVER"
    fi
    
    echo
    
    if [ -z "$SCHEMA" ]; then
        java -cp "$CLASSPATH" TestDataLoader "$ENGINE" "$CSV_DIR" "$JDBC_URL" "$USERNAME" "$PASSWORD"
    else
        java -cp "$CLASSPATH" TestDataLoader "$ENGINE" "$CSV_DIR" "$JDBC_URL" "$USERNAME" "$PASSWORD" "$SCHEMA"
    fi
else
    echo -e "${YELLOW}To run the loader, use:${NC}"
    echo "  ./load_test_data.sh <engine> <csv_dir> <jdbc_url> <username> <password> [schema]"
    echo
    echo "Or run directly:"
    echo "  java -cp \".:driver.jar\" TestDataLoader <engine> <csv_dir> <jdbc_url> <username> <password> [schema]"
fi
