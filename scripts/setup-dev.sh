#!/bin/bash
# Development environment setup script for claude-spark-sbt
# This script automates the setup process for new developers

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Functions
print_header() {
    echo ""
    echo -e "${CYAN}===================================================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}===================================================================${NC}"
    echo ""
}

print_step() {
    echo -e "${BLUE}➜${NC} $1"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

check_command() {
    if command -v "$1" &> /dev/null; then
        print_success "$1 is installed: $(command -v $1)"
        return 0
    else
        print_error "$1 is not installed"
        return 1
    fi
}

print_header "Claude Spark SBT - Development Environment Setup"

echo "This script will set up your development environment."
echo "It will:"
echo "  1. Check prerequisites (Java, Scala, SBT, Docker)"
echo "  2. Install git hooks"
echo "  3. Create .env file"
echo "  4. Start Docker services"
echo "  5. Compile the project"
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Setup cancelled."
    exit 1
fi

# Step 1: Check prerequisites
print_header "Step 1: Checking Prerequisites"

ALL_DEPS_OK=true

print_step "Checking Java..."
if check_command java; then
    JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo "  Version: $JAVA_VERSION"
    if [[ ! $JAVA_VERSION =~ ^11\. ]] && [[ ! $JAVA_VERSION =~ ^1\.11\. ]]; then
        print_warning "Java 11 is required, found: $JAVA_VERSION"
        ALL_DEPS_OK=false
    fi
else
    ALL_DEPS_OK=false
fi

print_step "Checking Scala..."
if check_command scala; then
    SCALA_VERSION=$(scala -version 2>&1 | awk '{print $5}')
    echo "  Version: $SCALA_VERSION"
else
    print_warning "Scala not found (SBT will download it automatically)"
fi

print_step "Checking SBT..."
if check_command sbt; then
    SBT_VERSION=$(sbt --version 2>&1 | grep 'sbt version' | awk '{print $4}')
    echo "  Version: $SBT_VERSION"
else
    print_error "SBT is required but not installed"
    echo "  Install from: https://www.scala-sbt.org/download.html"
    ALL_DEPS_OK=false
fi

print_step "Checking Docker..."
if check_command docker; then
    DOCKER_VERSION=$(docker --version | awk '{print $3}' | sed 's/,//')
    echo "  Version: $DOCKER_VERSION"

    # Check if Docker daemon is running
    if docker ps &> /dev/null; then
        print_success "Docker daemon is running"
    else
        print_warning "Docker daemon is not running"
        echo "  Please start Docker Desktop or the Docker daemon"
    fi
else
    print_warning "Docker not found (optional, needed for integration tests)"
fi

print_step "Checking Docker Compose..."
if check_command docker-compose; then
    COMPOSE_VERSION=$(docker-compose --version | awk '{print $4}' | sed 's/,//')
    echo "  Version: $COMPOSE_VERSION"
else
    print_warning "Docker Compose not found (optional, needed for integration tests)"
fi

print_step "Checking Git..."
if check_command git; then
    GIT_VERSION=$(git --version | awk '{print $3}')
    echo "  Version: $GIT_VERSION"
else
    print_error "Git is required"
    ALL_DEPS_OK=false
fi

if [ "$ALL_DEPS_OK" = false ]; then
    echo ""
    print_error "Some required dependencies are missing. Please install them and try again."
    exit 1
fi

# Step 2: Install git hooks
print_header "Step 2: Installing Git Hooks"

if [ -f ".git-hooks/install.sh" ]; then
    print_step "Running git hooks installer..."
    bash .git-hooks/install.sh
    print_success "Git hooks installed"
else
    print_warning "Git hooks installer not found at .git-hooks/install.sh"
fi

# Step 3: Create .env file
print_header "Step 3: Creating Environment Configuration"

if [ -f ".env" ]; then
    print_warning ".env file already exists, skipping creation"
    read -p "Do you want to overwrite it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_step "Creating .env file from template..."
        cp .env.example .env
        print_success ".env file created"
    fi
else
    print_step "Creating .env file from template..."
    cp .env.example .env
    print_success ".env file created"
fi

print_warning "Please edit .env file with your actual credentials before running pipelines"

# Step 4: Start Docker services (optional)
print_header "Step 4: Starting Docker Services"

if command -v docker &> /dev/null && command -v docker-compose &> /dev/null; then
    read -p "Do you want to start Docker services now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_step "Starting Docker services..."
        docker-compose up -d
        print_success "Docker services started"
        echo ""
        echo "Available services:"
        echo "  - Kafka UI:     http://localhost:8080"
        echo "  - PostgreSQL:   localhost:5432"
        echo "  - MySQL:        localhost:3306"
        echo "  - LocalStack:   http://localhost:4566"
        echo ""
        print_step "Waiting for services to be ready (30 seconds)..."
        sleep 30
        print_success "Services should be ready"
    else
        print_warning "Skipped Docker services. Start them later with: docker-compose up -d"
    fi
else
    print_warning "Docker not available, skipping service startup"
fi

# Step 5: Compile project
print_header "Step 5: Compiling Project"

read -p "Do you want to compile the project now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_step "Compiling project (this may take a few minutes on first run)..."
    if sbt compile; then
        print_success "Project compiled successfully"
    else
        print_error "Compilation failed"
        echo "  Please check the error messages above"
    fi
else
    print_warning "Skipped compilation. Compile later with: sbt compile"
fi

# Step 6: Run tests (optional)
print_header "Step 6: Running Tests"

read -p "Do you want to run tests now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    print_step "Running tests..."
    if sbt test; then
        print_success "All tests passed"
    else
        print_error "Some tests failed"
        echo "  Please check the test output above"
    fi
else
    print_warning "Skipped tests. Run them later with: sbt test"
fi

# Summary
print_header "Setup Complete!"

echo -e "${GREEN}Your development environment is ready!${NC}"
echo ""
echo "Quick reference:"
echo "  make help              - Show available make targets"
echo "  make compile           - Compile the project"
echo "  make test              - Run all tests"
echo "  make docker-up         - Start Docker services"
echo "  make docker-down       - Stop Docker services"
echo "  make format            - Format code"
echo "  make lint              - Run code quality checks"
echo "  make coverage          - Generate coverage report"
echo "  make assembly          - Build deployment JAR"
echo ""
echo "Documentation:"
echo "  README.md              - Project overview"
echo "  CONTRIBUTING.md        - Contribution guidelines"
echo "  IMPROVEMENTS.md        - Recent improvements"
echo "  API_DOCUMENTATION.md   - API reference"
echo ""
echo -e "${CYAN}Happy coding! 🚀${NC}"
