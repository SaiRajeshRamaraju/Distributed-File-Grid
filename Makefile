# Distributed File Grid - Makefile
# Build and management commands for the distributed storage system

.PHONY: all build clean install-deps start stop status test help

# Default target
all: build

# Build the project
build:
	@echo "Building Distributed File Grid..."
	@mkdir -p build
	@cd build && cmake .. && make -j$$(nproc)
	@echo "Build completed successfully!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf build/
	@rm -rf /tmp/chunks/
	@rm -rf /tmp/cluster_storage/
	@echo "Clean completed!"

# Install system dependencies (Ubuntu/Debian)
install-deps:
	@echo "Installing system dependencies..."
	@sudo apt-get update
	@sudo apt-get install -y build-essential cmake pkg-config \
		libprotobuf-dev protobuf-compiler \
		redis-server redis-tools \
		libfmt-dev zlib1g-dev
	@echo "Dependencies installed!"

# Install system dependencies (Arch Linux)
install-deps-arch:
	@echo "Installing system dependencies for Arch Linux..."
	@sudo pacman -S --needed base-devel cmake pkg-config \
		protobuf redis fmt zlib
	@echo "Dependencies installed!"

# Start all services
start: build
	@echo "Starting all services..."
	@./scripts/start_services.sh

# Stop all services
stop:
	@echo "Stopping all services..."
	@./scripts/stop_services.sh

# Check service status
status:
	@./scripts/status.sh

# Run system tests
test: build
	@echo "Running system tests..."
	@cd build && ./dfg test

# Upload a test file
upload-test: build
	@echo "Creating and uploading test file..."
	@echo "This is a test file for the distributed storage system." > /tmp/test_upload.txt
	@echo "It demonstrates file upload functionality." >> /tmp/test_upload.txt
	@cd build && ./dfg upload /tmp/test_upload.txt test_upload.txt

# Download the test file
download-test: build
	@echo "Downloading test file..."
	@cd build && ./dfg download test_upload.txt /tmp/test_download.txt
	@echo "Downloaded file contents:"
	@cat /tmp/test_download.txt

# Development setup
dev: install-deps build
	@echo "Development environment ready!"
	@echo "Run 'make start' to start all services"
	@echo "Run 'make test' to run tests"

# Show help
help:
	@echo "Distributed File Grid - Available Commands:"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build          - Build the project"
	@echo "  make clean          - Clean build artifacts"
	@echo "  make install-deps   - Install system dependencies (Ubuntu/Debian)"
	@echo "  make install-deps-arch - Install system dependencies (Arch Linux)"
	@echo ""
	@echo "Service Management:"
	@echo "  make start          - Start all services"
	@echo "  make stop           - Stop all services"
	@echo "  make status         - Check service status"
	@echo ""
	@echo "Testing:"
	@echo "  make test           - Run system tests"
	@echo "  make upload-test    - Upload a test file"
	@echo "  make download-test  - Download the test file"
	@echo ""
	@echo "Development:"
	@echo "  make dev            - Set up development environment"
	@echo "  make all            - Build everything (default)"
	@echo "  make help           - Show this help message"
	@echo ""
	@echo "Manual Commands:"
	@echo "  cd build && ./dfg head-server"
	@echo "  cd build && ./dfg cluster-server --server-id 1 --port 8080"
	@echo "  cd build && ./dfg upload <file> <name>"
	@echo "  cd build && ./dfg download <name> <output>"
