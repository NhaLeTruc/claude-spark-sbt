#!/bin/bash
# Troubleshooting script for claude-spark-sbt
# Diagnoses common issues and provides solutions

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

print_header() {
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
    echo ""
}

print_check() {
    echo -ne "${BLUE}→${NC} Checking $1... "
}

print_ok() {
    echo -e "${GREEN}OK${NC}"
}

print_fail() {
    echo -e "${RED}FAIL${NC}"
}

print_warn() {
    echo -e "${YELLOW}WARN${NC}"
}

print_solution() {
    echo -e "  ${YELLOW}Solution:${NC} $1"
}

print_header "Claude Spark SBT - Troubleshooting Tool"

ISSUES_FOUND=0

# Check Java
print_check "Java installation"
if command -v java &> /dev/null; then
    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    if [[ $JAVA_VERSION =~ ^11\. ]] || [[ $JAVA_VERSION =~ ^1\.11\. ]]; then
        print_ok
        echo "  Version: $JAVA_VERSION"
    else
        print_warn
        echo "  Found: $JAVA_VERSION (Expected: 11.x)"
        print_solution "Install Java 11 (LTS): https://adoptium.net/"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi
else
    print_fail
    print_solution "Install Java 11 from https://adoptium.net/"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi

# Check SBT
print_check "SBT installation"
if command -v sbt &> /dev/null; then
    print_ok
    SBT_VERSION=$(sbt --version 2>&1 | grep 'sbt version' | awk '{print $4}' || echo "unknown")
    echo "  Version: $SBT_VERSION"
else
    print_fail
    print_solution "Install SBT: https://www.scala-sbt.org/download.html"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi

# Check Docker
print_check "Docker installation"
if command -v docker &> /dev/null; then
    print_ok
    DOCKER_VERSION=$(docker --version | awk '{print $3}' | sed 's/,//')
    echo "  Version: $DOCKER_VERSION"
else
    print_warn
    print_solution "Install Docker for integration tests: https://docs.docker.com/get-docker/"
fi

# Check Docker daemon
print_check "Docker daemon status"
if command -v docker &> /dev/null; then
    if docker ps &> /dev/null; then
        print_ok
    else
        print_fail
        print_solution "Start Docker Desktop or run: sudo systemctl start docker"
        ISSUES_FOUND=$((ISSUES_FOUND + 1))
    fi
else
    echo "skipped"
fi

# Check Docker Compose
print_check "Docker Compose installation"
if command -v docker-compose &> /dev/null; then
    print_ok
    COMPOSE_VERSION=$(docker-compose --version | awk '{print $4}' | sed 's/,//')
    echo "  Version: $COMPOSE_VERSION"
else
    print_warn
    print_solution "Install Docker Compose: https://docs.docker.com/compose/install/"
fi

# Check .env file
print_check ".env file"
if [ -f ".env" ]; then
    print_ok
else
    print_warn
    print_solution "Create .env file: cp .env.example .env"
fi

# Check git hooks
print_check "Git hooks"
if [ -f ".git/hooks/pre-commit" ]; then
    print_ok
else
    print_warn
    print_solution "Install hooks: ./.git-hooks/install.sh"
fi

# Check project structure
print_check "Project structure"
REQUIRED_DIRS=("src/main/scala" "src/test/scala" "project" "src/main/resources")
ALL_DIRS_OK=true
for dir in "${REQUIRED_DIRS[@]}"; do
    if [ ! -d "$dir" ]; then
        ALL_DIRS_OK=false
        break
    fi
done

if [ "$ALL_DIRS_OK" = true ]; then
    print_ok
else
    print_fail
    print_solution "Ensure you're in the project root directory"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi

# Check build.sbt
print_check "build.sbt configuration"
if [ -f "build.sbt" ]; then
    print_ok
else
    print_fail
    print_solution "Restore build.sbt from version control"
    ISSUES_FOUND=$((ISSUES_FOUND + 1))
fi

# Check Docker services
if command -v docker &> /dev/null && docker ps &> /dev/null; then
    print_header "Docker Services Status"

    services=("etl-kafka" "etl-postgres" "etl-mysql" "etl-localstack")
    for service in "${services[@]}"; do
        print_check "$service"
        if docker ps --format '{{.Names}}' | grep -q "^${service}$"; then
            # Check if service is healthy
            health=$(docker inspect --format='{{.State.Health.Status}}' $service 2>/dev/null || echo "no-health-check")
            if [ "$health" = "healthy" ]; then
                print_ok
            elif [ "$health" = "no-health-check" ]; then
                print_ok
                echo "  (no health check)"
            else
                print_warn
                echo "  Status: $health"
                print_solution "Wait for service to become healthy or restart: docker-compose restart $service"
            fi
        else
            print_warn
            print_solution "Start services: docker-compose up -d"
        fi
    done
fi

# Check common port conflicts
print_header "Port Availability"

check_port() {
    PORT=$1
    SERVICE=$2
    print_check "Port $PORT ($SERVICE)"
    if lsof -i:$PORT &> /dev/null || netstat -tuln 2>/dev/null | grep -q ":$PORT "; then
        print_warn
        echo "  Port is already in use"
        print_solution "Stop the service using this port or change configuration"
    else
        print_ok
    fi
}

if command -v lsof &> /dev/null || command -v netstat &> /dev/null; then
    check_port 8080 "Kafka UI"
    check_port 5432 "PostgreSQL"
    check_port 3306 "MySQL"
    check_port 4566 "LocalStack"
    check_port 8888 "Health Check"
else
    echo "Port check tools not available (lsof/netstat)"
fi

# Check compilation
print_header "Project Compilation"

if command -v sbt &> /dev/null; then
    print_check "Compilation status"
    if [ -d "target/scala-2.12/classes" ]; then
        print_ok
        echo "  Project has been compiled"
    else
        print_warn
        print_solution "Compile project: sbt compile"
    fi

    # Check for common compilation errors
    if [ -f "target/streams/compile/compileIncremental/_global/streams/out" ]; then
        if grep -q "error" target/streams/compile/compileIncremental/_global/streams/out 2>/dev/null; then
            print_warn
            echo "  Recent compilation errors found"
            print_solution "Run: sbt clean compile"
        fi
    fi
fi

# Check test compilation
print_check "Test compilation"
if [ -d "target/scala-2.12/test-classes" ]; then
    print_ok
else
    print_warn
    print_solution "Compile tests: sbt Test/compile"
fi

# Memory check
print_header "System Resources"

print_check "Available memory"
if command -v free &> /dev/null; then
    AVAILABLE_MEM=$(free -m | awk 'NR==2{print $7}')
    if [ $AVAILABLE_MEM -gt 4096 ]; then
        print_ok
        echo "  Available: ${AVAILABLE_MEM}MB"
    else
        print_warn
        echo "  Available: ${AVAILABLE_MEM}MB (Recommended: >4GB)"
        print_solution "Close other applications or increase system memory"
    fi
elif command -v vm_stat &> /dev/null; then
    # macOS
    print_ok
    echo "  (macOS detected)"
else
    echo "  (unable to check)"
fi

# Disk space check
print_check "Disk space"
if command -v df &> /dev/null; then
    AVAILABLE_DISK=$(df -h . | awk 'NR==2{print $4}')
    print_ok
    echo "  Available: $AVAILABLE_DISK"
else
    echo "  (unable to check)"
fi

# Summary
print_header "Summary"

if [ $ISSUES_FOUND -eq 0 ]; then
    echo -e "${GREEN}✓ No critical issues found!${NC}"
    echo ""
    echo "If you're still experiencing problems:"
    echo "  1. Check the logs: docker-compose logs"
    echo "  2. Review TROUBLESHOOTING.md for specific errors"
    echo "  3. Try clean build: sbt clean compile"
    echo "  4. Restart Docker services: make docker-restart"
else
    echo -e "${YELLOW}⚠ Found $ISSUES_FOUND issue(s) that need attention${NC}"
    echo ""
    echo "Please resolve the issues above and run this script again."
fi

echo ""
echo "For more help:"
echo "  - TROUBLESHOOTING.md - Common issues and solutions"
echo "  - README.md - Project documentation"
echo "  - make help - Available make targets"
