# Makefile for claude-spark-sbt ETL framework
# Simplifies common development tasks

.PHONY: help setup compile test clean format lint coverage docker-up docker-down assembly run install-hooks check all

# Default target
.DEFAULT_GOAL := help

# Colors for output
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

##@ General

help: ## Display this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\n${CYAN}Usage:${NC}\n  make ${GREEN}<target>${NC}\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  ${GREEN}%-15s${NC} %s\n", $$1, $$2 } /^##@/ { printf "\n${CYAN}%s${NC}\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

setup: install-hooks env ## Setup development environment
	@echo "${GREEN}Development environment ready!${NC}"
	@echo "${YELLOW}Next steps:${NC}"
	@echo "  1. Edit .env with your credentials"
	@echo "  2. Run 'make docker-up' to start services"
	@echo "  3. Run 'make compile' to build project"

install-hooks: ## Install git pre-commit hooks
	@echo "${CYAN}Installing git hooks...${NC}"
	@./.git-hooks/install.sh

env: ## Create .env file from template
	@if [ ! -f .env ]; then \
		echo "${CYAN}Creating .env file from template...${NC}"; \
		cp .env.example .env; \
		echo "${GREEN}.env file created${NC}"; \
		echo "${YELLOW}Please edit .env with your credentials${NC}"; \
	else \
		echo "${YELLOW}.env file already exists${NC}"; \
	fi

##@ Build

compile: ## Compile the project
	@echo "${CYAN}Compiling project...${NC}"
	@sbt compile

assembly: ## Build fat JAR for deployment
	@echo "${CYAN}Building assembly JAR...${NC}"
	@sbt assembly
	@echo "${GREEN}JAR built: target/scala-2.12/claude-spark-etl-1.0.0.jar${NC}"

clean: ## Clean build artifacts
	@echo "${CYAN}Cleaning build artifacts...${NC}"
	@sbt clean
	@echo "${GREEN}Clean complete${NC}"

##@ Testing

test: ## Run all tests
	@echo "${CYAN}Running all tests...${NC}"
	@sbt test

test-unit: ## Run unit tests only
	@echo "${CYAN}Running unit tests...${NC}"
	@sbt "testOnly *.unit.*"

test-integration: docker-up ## Run integration tests with Docker
	@echo "${CYAN}Running integration tests...${NC}"
	@sbt "testOnly *.integration.*"

test-contract: ## Run contract tests
	@echo "${CYAN}Running contract tests...${NC}"
	@sbt "testOnly *.contract.*"

test-watch: ## Run tests in watch mode
	@echo "${CYAN}Running tests in watch mode (press Enter to re-run)...${NC}"
	@sbt ~test

coverage: ## Generate code coverage report
	@echo "${CYAN}Generating coverage report...${NC}"
	@sbt clean coverage test coverageReport
	@echo "${GREEN}Coverage report: target/scala-2.12/scoverage-report/index.html${NC}"

coverage-open: coverage ## Generate and open coverage report
	@echo "${CYAN}Opening coverage report...${NC}"
	@open target/scala-2.12/scoverage-report/index.html || xdg-open target/scala-2.12/scoverage-report/index.html

##@ Code Quality

format: ## Format code with scalafmt
	@echo "${CYAN}Formatting code...${NC}"
	@sbt scalafmtAll scalafmtSbt

format-check: ## Check code formatting
	@echo "${CYAN}Checking code formatting...${NC}"
	@sbt scalafmtCheckAll scalafmtSbtCheck

lint: ## Run scalastyle linter
	@echo "${CYAN}Running scalastyle...${NC}"
	@sbt scalastyle

check: format-check lint compile test ## Run all quality checks
	@echo "${GREEN}All quality checks passed!${NC}"

##@ Docker

docker-up: ## Start Docker services
	@echo "${CYAN}Starting Docker services...${NC}"
	@docker-compose up -d
	@echo "${GREEN}Docker services started${NC}"
	@echo "Services:"
	@echo "  - Kafka UI: http://localhost:8080"
	@echo "  - PostgreSQL: localhost:5432"
	@echo "  - MySQL: localhost:3306"
	@echo "  - LocalStack S3: http://localhost:4566"

docker-down: ## Stop Docker services
	@echo "${CYAN}Stopping Docker services...${NC}"
	@docker-compose down

docker-logs: ## View Docker logs
	@docker-compose logs -f

docker-ps: ## Show Docker services status
	@docker-compose ps

docker-clean: docker-down ## Clean Docker volumes
	@echo "${CYAN}Cleaning Docker volumes...${NC}"
	@docker-compose down -v
	@echo "${GREEN}Docker volumes cleaned${NC}"

docker-restart: docker-down docker-up ## Restart Docker services
	@echo "${GREEN}Docker services restarted${NC}"

##@ Running

run-batch: assembly ## Run example batch pipeline
	@echo "${CYAN}Running example batch pipeline...${NC}"
	@spark-submit \
		--class com.etl.Main \
		--master local[4] \
		target/scala-2.12/claude-spark-etl-1.0.0.jar \
		--config src/main/resources/configs/example-batch-pipeline.json \
		--mode batch

run-streaming: assembly ## Run example streaming pipeline
	@echo "${CYAN}Running example streaming pipeline...${NC}"
	@spark-submit \
		--class com.etl.Main \
		--master local[4] \
		target/scala-2.12/claude-spark-etl-1.0.0.jar \
		--config src/main/resources/configs/example-streaming-pipeline.json \
		--mode streaming

##@ Documentation

docs: ## Generate Scaladoc
	@echo "${CYAN}Generating Scaladoc...${NC}"
	@sbt doc
	@echo "${GREEN}Documentation: target/scala-2.12/api/index.html${NC}"

docs-open: docs ## Generate and open Scaladoc
	@echo "${CYAN}Opening Scaladoc...${NC}"
	@open target/scala-2.12/api/index.html || xdg-open target/scala-2.12/api/index.html

##@ CI/CD

ci: clean check coverage ## Run CI pipeline locally
	@echo "${GREEN}CI pipeline completed successfully!${NC}"

release: clean check coverage assembly ## Prepare release
	@echo "${GREEN}Release artifacts ready:${NC}"
	@echo "  - JAR: target/scala-2.12/claude-spark-etl-1.0.0.jar"
	@echo "  - Coverage: target/scala-2.12/scoverage-report/index.html"

##@ Benchmarking

benchmark: ## Run all JMH benchmarks
	@echo "${CYAN}Running all JMH benchmarks...${NC}"
	@sbt "Jmh/run -i 10 -wi 5 -f 1"

benchmark-quick: ## Run quick benchmarks (fewer iterations)
	@echo "${CYAN}Running quick benchmarks...${NC}"
	@sbt "Jmh/run -i 3 -wi 2 -f 1"

benchmark-pipeline: ## Run ETL pipeline benchmarks
	@echo "${CYAN}Running ETL pipeline benchmarks...${NC}"
	@sbt "Jmh/run -i 10 -wi 5 -f 1 ETLPipelineBenchmark"

benchmark-retry: ## Run retry and circuit breaker benchmarks
	@echo "${CYAN}Running retry/circuit breaker benchmarks...${NC}"
	@sbt "Jmh/run -i 10 -wi 5 -f 1 RetryCircuitBreakerBenchmark"

benchmark-profile: ## Run benchmarks with GC profiling
	@echo "${CYAN}Running benchmarks with GC profiling...${NC}"
	@sbt "Jmh/run -i 10 -wi 5 -f 1 -prof gc"

benchmark-list: ## List all available benchmarks
	@echo "${CYAN}Available benchmarks:${NC}"
	@sbt "Jmh/run -l"

benchmark-report: ## Run benchmarks and generate CSV report
	@echo "${CYAN}Running benchmarks and generating report...${NC}"
	@sbt "Jmh/run -i 10 -wi 5 -f 1 -rf csv -rff benchmark-results.csv"
	@echo "${GREEN}Report saved to: benchmark-results.csv${NC}"

##@ Utilities

console: ## Start Scala console with project classpath
	@echo "${CYAN}Starting Scala console...${NC}"
	@sbt console

dependency-tree: ## Show dependency tree
	@echo "${CYAN}Generating dependency tree...${NC}"
	@sbt dependencyTree

dependency-updates: ## Check for dependency updates
	@echo "${CYAN}Checking for dependency updates...${NC}"
	@sbt dependencyUpdates

stats: ## Show project statistics
	@echo "${CYAN}Project Statistics:${NC}"
	@echo ""
	@echo "${GREEN}Source Files:${NC}"
	@find src/main/scala -name "*.scala" | wc -l | xargs echo "  Scala files:"
	@find src/test/scala -name "*.scala" | wc -l | xargs echo "  Test files:"
	@echo ""
	@echo "${GREEN}Lines of Code:${NC}"
	@find src/main/scala -name "*.scala" -exec cat {} \; | wc -l | xargs echo "  Source LoC:"
	@find src/test/scala -name "*.scala" -exec cat {} \; | wc -l | xargs echo "  Test LoC:"
	@echo ""
	@echo "${GREEN}Configurations:${NC}"
	@find src/main/resources/configs -name "*.json" | wc -l | xargs echo "  Example configs:"
	@find src/main/resources/schemas -name "*.avsc" | wc -l | xargs echo "  Avro schemas:"

all: clean check coverage assembly ## Build everything
	@echo "${GREEN}Build complete!${NC}"

.PHONY: setup-dev
setup-dev: setup docker-up compile test ## Complete development setup
	@echo "${GREEN}Development environment fully configured!${NC}"
	@echo "${YELLOW}You're ready to start developing!${NC}"
