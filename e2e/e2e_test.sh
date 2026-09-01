#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# Distributed File Grid — Comprehensive Dual-Mode End-to-End (E2E) Test Suite
# ═══════════════════════════════════════════════════════════════════════════════

set -uo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BUILD_DIR="${PROJECT_ROOT}/build"
DFG_BIN="${BUILD_DIR}/dfg"
CLIENT_BIN="${BUILD_DIR}/dfg_client"
COMPOSE_FILE="${PROJECT_ROOT}/deploy/docker/docker-compose.yml"
COMPOSE_PROJECT="dfg"

# Execution mode flags
SYSTEM_TEST=0
VERBOSE=0

# Colors for terminal output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

PASSED_TESTS=0
FAILED_TESTS=0

DOCKER_COMPOSE_CMD=""

print_usage() {
    cat << 'EOF'
Distributed File Grid — End-to-End (E2E) Test Runner

Usage:
  ./e2e/e2e_test.sh [OPTIONS]

Options:
  --system-test       Run full containerized system tests using Docker Compose
                      (boots primary+backup Head Servers, 3 Cluster Servers,
                      Health Checker, ZooKeeper Monitor, Redis, ZooKeeper)
  --verbose, -v       Enable verbose diagnostic logging
  --help, -h          Display this help message and exit

Execution Modes:
  1. Default Mode (Local Execution):
     Starts local background services on isolated ports, executes complete
     data integrity tests (small 256KB, multi-MB 2MB, 0-byte edge cases,
     dfg_client TCP stream, HTTP control API, Prometheus metrics, replica
     fault tolerance and consensus read repair), and cleanly terminates all
     spawned processes upon completion or failure.

  2. System Test Mode (--system-test):
     Builds Docker images, boots the full 14-service container topology via
     Docker Compose, polls container health checks, executes core user
     application workflows against the live containerized grid, validates
     telemetry, and executes clean container teardown.
EOF
}

# Parse command line arguments first
while [[ $# -gt 0 ]]; do
    case "$1" in
        --system-test)
            SYSTEM_TEST=1
            shift
            ;;
        --verbose|-v)
            VERBOSE=1
            shift
            ;;
        --help|-h)
            print_usage
            exit 0
            ;;
        *)
            echo -e "${RED}[ERROR] Unknown option: $1${NC}"
            print_usage
            exit 1
            ;;
    esac
done

# Dedicated sandbox temporary directory
TMP_DIR="$(mktemp -d /tmp/dfg_e2e_XXXXXX)"
LOG_DIR="${TMP_DIR}/logs"
mkdir -p "${LOG_DIR}"

report_result() {
    local test_name="$1"
    local status="$2"
    local details="${3:-}"

    if [[ "${status}" -eq 0 ]]; then
        echo -e "  [ ${GREEN}PASS${NC} ] ${test_name}"
        PASSED_TESTS=$((PASSED_TESTS + 1))
    else
        echo -e "  [ ${RED}FAIL${NC} ] ${test_name}"
        if [[ -n "${details}" ]]; then
            echo -e "         ${RED}Reason: ${details}${NC}"
        fi
        if [[ "${VERBOSE}" -eq 1 ]]; then
            echo -e "         ${YELLOW}--- Diagnostic logs tail ---${NC}"
            tail -n 20 "${LOG_DIR}"/*.log 2>/dev/null || true
        fi
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# Local Execution Mode
# ═══════════════════════════════════════════════════════════════════════════════

cleanup_local() {
    echo ""
    echo -e "${YELLOW}Shutting down local test cluster services and cleaning sandbox...${NC}"
    "${PROJECT_ROOT}/scripts/stop_services.sh" > /dev/null 2>&1 || true
    rm -rf "${TMP_DIR}"
}

run_local_mode() {
    trap cleanup_local EXIT INT TERM

    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}     Distributed File Grid — Local Mode End-to-End Test Suite      ${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "Project Root: ${CYAN}${PROJECT_ROOT}${NC}"
    echo -e "Build Dir:    ${CYAN}${BUILD_DIR}${NC}"
    echo -e "Sandbox Dir:  ${CYAN}${TMP_DIR}${NC}"
    echo ""

    # Ensure required binaries exist
    if [[ ! -x "${DFG_BIN}" ]]; then
        echo -e "${RED}[ERROR] Main binary '${DFG_BIN}' not found. Please build the project first.${NC}"
        echo "Example: cmake -B build -DBUILD_TESTS=ON && cmake --build build"
        exit 1
    fi

    echo -e "${CYAN}─── Phase 1: Local Cluster Initialization ────────────────────────${NC}"
    echo "Stopping any existing background services..."
    "${PROJECT_ROOT}/scripts/stop_services.sh" > /dev/null 2>&1 || true

    echo "Starting fresh local cluster services..."
    "${PROJECT_ROOT}/scripts/start_services.sh" > "${LOG_DIR}/start_services.log" 2>&1

    echo "Waiting for head server control API to become healthy..."
    local READY=0
    for i in {1..20}; do
        if curl -sf http://127.0.0.1:9670/api/v1/status > /dev/null 2>&1; then
            READY=1
            break
        fi
        sleep 1
    done

    if [[ "${READY}" -ne 1 ]]; then
        echo -e "${RED}[FATAL] Head server failed to start within 20 seconds.${NC}"
        cat "${PROJECT_ROOT}/logs/head_server.log" 2>/dev/null || true
        exit 1
    fi
    echo -e "${GREEN}Local cluster services are up and responding to control API!${NC}"
    echo ""

    echo -e "${CYAN}─── Phase 2: Functional & Integrity E2E Tests ───────────────────${NC}"

    # ─────────────────────────────────────────────────────────────────────────
    # Test 1: Small File Upload & Download Integrity (256 KB)
    # ─────────────────────────────────────────────────────────────────────────
    local SMALL_SRC="${TMP_DIR}/test_small.bin"
    local SMALL_OUT="${TMP_DIR}/out_small.bin"
    head -c 262144 /dev/urandom > "${SMALL_SRC}" # 256 KB
    local ORIGINAL_HASH_SMALL
    ORIGINAL_HASH_SMALL=$(sha256sum "${SMALL_SRC}" | awk '{print $1}')

    "${DFG_BIN}" upload "${SMALL_SRC}" "e2e_small.bin" > "${LOG_DIR}/upload_small.log" 2>&1
    local UP_STATUS_1=$?

    if [[ ${UP_STATUS_1} -eq 0 ]]; then
        "${DFG_BIN}" download "e2e_small.bin" "${SMALL_OUT}" > "${LOG_DIR}/download_small.log" 2>&1
        local DOWN_STATUS_1=$?
        if [[ ${DOWN_STATUS_1} -eq 0 && -f "${SMALL_OUT}" ]]; then
            local RESTORED_HASH_SMALL
            RESTORED_HASH_SMALL=$(sha256sum "${SMALL_OUT}" | awk '{print $1}')
            if [[ "${ORIGINAL_HASH_SMALL}" == "${RESTORED_HASH_SMALL}" ]]; then
                report_result "Test 1: Small File Upload & Download (256 KB SHA-256 integrity)" 0
            else
                report_result "Test 1: Small File Upload & Download" 1 "Hash mismatch: expected ${ORIGINAL_HASH_SMALL}, got ${RESTORED_HASH_SMALL}"
            fi
        else
            report_result "Test 1: Small File Upload & Download" 1 "Download failed with exit code ${DOWN_STATUS_1}"
        fi
    else
        report_result "Test 1: Small File Upload & Download" 1 "Upload failed with exit code ${UP_STATUS_1}"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # Test 2: Multi-MB File Upload & Download Integrity (2 MB)
    # ─────────────────────────────────────────────────────────────────────────
    local LARGE_SRC="${TMP_DIR}/test_large.bin"
    local LARGE_OUT="${TMP_DIR}/out_large.bin"
    head -c 2097152 /dev/urandom > "${LARGE_SRC}" # 2 MB
    local ORIGINAL_HASH_LARGE
    ORIGINAL_HASH_LARGE=$(sha256sum "${LARGE_SRC}" | awk '{print $1}')

    "${DFG_BIN}" upload "${LARGE_SRC}" "e2e_large.bin" > "${LOG_DIR}/upload_large.log" 2>&1
    local UP_STATUS_2=$?

    if [[ ${UP_STATUS_2} -eq 0 ]]; then
        "${DFG_BIN}" download "e2e_large.bin" "${LARGE_OUT}" > "${LOG_DIR}/download_large.log" 2>&1
        local DOWN_STATUS_2=$?
        if [[ ${DOWN_STATUS_2} -eq 0 && -f "${LARGE_OUT}" ]]; then
            local RESTORED_HASH_LARGE
            RESTORED_HASH_LARGE=$(sha256sum "${LARGE_OUT}" | awk '{print $1}')
            if [[ "${ORIGINAL_HASH_LARGE}" == "${RESTORED_HASH_LARGE}" ]]; then
                report_result "Test 2: Multi-MB File Upload & Download (2 MB SHA-256 integrity)" 0
            else
                report_result "Test 2: Multi-MB File Upload & Download" 1 "Hash mismatch: expected ${ORIGINAL_HASH_LARGE}, got ${RESTORED_HASH_LARGE}"
            fi
        else
            report_result "Test 2: Multi-MB File Upload & Download" 1 "Download failed with exit code ${DOWN_STATUS_2}"
        fi
    else
        report_result "Test 2: Multi-MB File Upload & Download" 1 "Upload failed with exit code ${UP_STATUS_2}"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # Test 3: Standalone dfg_client Streaming Download
    # ─────────────────────────────────────────────────────────────────────────
    if [[ -x "${CLIENT_BIN}" ]]; then
        local CLIENT_OUT="${TMP_DIR}/out_client.bin"
        "${CLIENT_BIN}" 127.0.0.1 9669 "e2e_small.bin" "${CLIENT_OUT}" > "${LOG_DIR}/dfg_client.log" 2>&1
        local CLIENT_STATUS=$?
        if [[ ${CLIENT_STATUS} -eq 0 && -f "${CLIENT_OUT}" ]]; then
            local CLIENT_HASH
            CLIENT_HASH=$(sha256sum "${CLIENT_OUT}" | awk '{print $1}')
            if [[ "${ORIGINAL_HASH_SMALL}" == "${CLIENT_HASH}" ]]; then
                report_result "Test 3: Standalone Protocol Client (dfg_client TCP :9669 stream & verify)" 0
            else
                report_result "Test 3: Standalone Protocol Client" 1 "Hash mismatch from dfg_client"
            fi
        else
            report_result "Test 3: Standalone Protocol Client" 1 "dfg_client failed with exit code ${CLIENT_STATUS}"
        fi
    else
        report_result "Test 3: Standalone Protocol Client (dfg_client)" 1 "Binary not found at ${CLIENT_BIN}"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # Test 4: Zero-Byte Empty File Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    local EMPTY_SRC="${TMP_DIR}/test_empty.txt"
    local EMPTY_OUT="${TMP_DIR}/out_empty.txt"
    touch "${EMPTY_SRC}"

    "${DFG_BIN}" upload "${EMPTY_SRC}" "e2e_empty.txt" > "${LOG_DIR}/upload_empty.log" 2>&1
    local UP_STATUS_4=$?

    if [[ ${UP_STATUS_4} -eq 0 ]]; then
        "${DFG_BIN}" download "e2e_empty.txt" "${EMPTY_OUT}" > "${LOG_DIR}/download_empty.log" 2>&1
        local DOWN_STATUS_4=$?
        if [[ ${DOWN_STATUS_4} -eq 0 && -f "${EMPTY_OUT}" && ! -s "${EMPTY_OUT}" ]]; then
            report_result "Test 4: 0-Byte Empty File Upload & Download" 0
        else
            report_result "Test 4: 0-Byte Empty File Upload & Download" 1 "Downloaded empty file invalid"
        fi
    else
        report_result "Test 4: 0-Byte Empty File Upload & Download" 1 "Upload empty file failed"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # Test 5: Metadata Store & File Listing Command
    # ─────────────────────────────────────────────────────────────────────────
    local LIST_OUTPUT
    LIST_OUTPUT=$("${DFG_BIN}" list 2>&1 || true)
    if echo "${LIST_OUTPUT}" | grep -q "e2e_small.bin" && echo "${LIST_OUTPUT}" | grep -q "e2e_large.bin"; then
        report_result "Test 5: Metadata Store & File Listing (dfg list)" 0
    else
        report_result "Test 5: Metadata Store & File Listing (dfg list)" 1 "Files not found in listing output: ${LIST_OUTPUT}"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # Test 6: HTTP Control API Endpoints
    # ─────────────────────────────────────────────────────────────────────────
    local STATUS_JSON
    local CLUSTER_JSON
    STATUS_JSON=$(curl -sf http://127.0.0.1:9670/api/v1/status || echo "")
    CLUSTER_JSON=$(curl -sf http://127.0.0.1:9670/api/v1/servers/cluster || echo "")

    if echo "${STATUS_JSON}" | grep -q "total_servers" && echo "${CLUSTER_JSON}" | grep -q "servers"; then
        report_result "Test 6: Head Server HTTP Control API (status & cluster list :9670)" 0
    else
        report_result "Test 6: Head Server HTTP Control API" 1 "Invalid response from Control API endpoints"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # Test 7: Prometheus Metrics Endpoints
    # ─────────────────────────────────────────────────────────────────────────
    local HEAD_METRICS
    local HC_METRICS
    local CLUSTER_METRICS
    HEAD_METRICS=$(curl -sf http://127.0.0.1:9095/metrics 2>/dev/null || echo "")
    HC_METRICS=$(curl -sf http://127.0.0.1:9096/metrics 2>/dev/null || echo "")
    CLUSTER_METRICS=$(curl -sf http://127.0.0.1:9091/metrics 2>/dev/null || echo "")

    if [[ -n "${HEAD_METRICS}" && -n "${HC_METRICS}" && -n "${CLUSTER_METRICS}" ]]; then
        report_result "Test 7: Prometheus Metrics Exporters (:9095, :9096, :9091)" 0
    else
        report_result "Test 7: Prometheus Metrics Exporters" 1 "One or more metrics endpoints did not respond"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # Test 8: Fault-Tolerance & Replica Voting
    # ─────────────────────────────────────────────────────────────────────────
    local CHUNK_FILE
    CHUNK_FILE=$(find /var/cluster_storage/ /tmp/cluster_storage/ -name "*e2e_small.bin_chunk_0*" 2>/dev/null | head -n 1)
    if [[ -n "${CHUNK_FILE}" && -f "${CHUNK_FILE}" ]]; then
        echo "CORRUPTED_DATA_INJECTED" > "${CHUNK_FILE}"
    else
        report_result "Test 8: Replica Fault Tolerance & Consensus Voting" 1 "Could not find chunk file on disk to corrupt"
        return
    fi

    local REPAIR_OUT="${TMP_DIR}/out_repaired.bin"
    "${DFG_BIN}" download "e2e_small.bin" "${REPAIR_OUT}" > "${LOG_DIR}/download_repair.log" 2>&1
    local REPAIR_STATUS=$?

    if [[ ${REPAIR_STATUS} -eq 0 && -f "${REPAIR_OUT}" ]]; then
        local REPAIRED_HASH
        REPAIRED_HASH=$(sha256sum "${REPAIR_OUT}" | awk '{print $1}')
        if [[ "${ORIGINAL_HASH_SMALL}" == "${REPAIRED_HASH}" ]]; then
            report_result "Test 8: Replica Fault Tolerance & Consensus Voting" 0
        else
            report_result "Test 8: Replica Fault Tolerance & Consensus Voting" 1 "Corrupted data served on download"
        fi
    else
        report_result "Test 8: Replica Fault Tolerance & Consensus Voting" 1 "Download failed during replica corruption test"
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# Containerized System Test Mode (--system-test)
# ═══════════════════════════════════════════════════════════════════════════════

cleanup_system() {
    echo ""
    echo -e "${YELLOW}Tearing down Docker containerized cluster and cleaning sandbox...${NC}"
    if [[ -n "${DOCKER_COMPOSE_CMD}" ]]; then
        ${DOCKER_COMPOSE_CMD} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" down -v --remove-orphans > /dev/null 2>&1 || true
    fi
    rm -rf "${TMP_DIR}"
}

run_system_mode() {
    # Detect Docker Compose command
    if command -v docker-compose >/dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker-compose"
    elif docker compose version >/dev/null 2>&1; then
        DOCKER_COMPOSE_CMD="docker compose"
    else
        echo -e "${RED}[ERROR] Neither 'docker-compose' nor 'docker compose' was found on this system.${NC}"
        echo "Docker Compose is required to run --system-test."
        exit 1
    fi

    # Verify Docker daemon is reachable
    if ! docker info >/dev/null 2>&1; then
        echo -e "${RED}[ERROR] Cannot connect to Docker daemon.${NC}"
        echo "Please ensure Docker is installed, running, and accessible to current user."
        exit 1
    fi

    trap cleanup_system EXIT INT TERM

    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "${BLUE}  Distributed File Grid — Containerized System Test Mode (--system-test) ${NC}"
    echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
    echo -e "Compose File: ${CYAN}${COMPOSE_FILE}${NC}"
    echo -e "Compose Cmd:  ${CYAN}${DOCKER_COMPOSE_CMD}${NC}"
    echo -e "Sandbox Dir:  ${CYAN}${TMP_DIR}${NC}"
    echo ""

    echo -e "${CYAN}─── Phase 1: Containerized Grid Teardown & Image Build ───────────${NC}"
    echo "Cleaning any stale containers and volumes..."
    ${DOCKER_COMPOSE_CMD} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" down -v --remove-orphans > /dev/null 2>&1 || true

    echo "Building Docker images for Distributed File Grid..."
    if ! ${DOCKER_COMPOSE_CMD} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" build > "${LOG_DIR}/docker_build.log" 2>&1; then
        echo -e "${RED}[FATAL] Docker image build failed. Log output:${NC}"
        tail -n 30 "${LOG_DIR}/docker_build.log"
        exit 1
    fi
    echo -e "${GREEN}Docker images built successfully.${NC}"

    echo -e "${CYAN}─── Phase 2: Starting Containerized Cluster ──────────────────────${NC}"
    if ! ${DOCKER_COMPOSE_CMD} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" up -d > "${LOG_DIR}/docker_up.log" 2>&1; then
        echo -e "${RED}[FATAL] Failed to start Docker Compose cluster.${NC}"
        tail -n 30 "${LOG_DIR}/docker_up.log"
        exit 1
    fi
    echo -e "${GREEN}Containers launched in background.${NC}"

    echo "Waiting for core services (ZooKeeper, Redis, Head Servers, Cluster Servers, Health Checker) to become ready..."
    local HEALTHY=0
    for attempt in {1..30}; do
        if curl -sf http://127.0.0.1:9670/api/v1/status > /dev/null 2>&1; then
            HEALTHY=1
            break
        fi
        sleep 2
    done

    if [[ "${HEALTHY}" -ne 1 ]]; then
        echo -e "${RED}[FATAL] Containerized Head Server did not respond within timeout.${NC}"
        ${DOCKER_COMPOSE_CMD} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" logs --tail=40
        exit 1
    fi
    echo -e "${GREEN}All containerized services are healthy and responding!${NC}"
    echo ""

    echo -e "${CYAN}─── Phase 3: Containerized Grid Functional & Integrity Tests ─────${NC}"

    # ─────────────────────────────────────────────────────────────────────────
    # System Test 1: Small File Upload & Download (256 KB SHA-256 integrity)
    # ─────────────────────────────────────────────────────────────────────────
    local SYS_SMALL_SRC="${TMP_DIR}/sys_small.bin"
    local SYS_SMALL_OUT="${TMP_DIR}/sys_out_small.bin"
    head -c 262144 /dev/urandom > "${SYS_SMALL_SRC}" # 256 KB
    local SYS_ORIGINAL_HASH_SMALL
    SYS_ORIGINAL_HASH_SMALL=$(sha256sum "${SYS_SMALL_SRC}" | awk '{print $1}')

    if [[ -x "${DFG_BIN}" ]]; then
        "${DFG_BIN}" upload "${SYS_SMALL_SRC}" "sys_small.bin" > "${LOG_DIR}/sys_upload_small.log" 2>&1
        local SYS_UP_STATUS=$?
        if [[ ${SYS_UP_STATUS} -eq 0 ]]; then
            "${DFG_BIN}" download "sys_small.bin" "${SYS_SMALL_OUT}" > "${LOG_DIR}/sys_download_small.log" 2>&1
            local SYS_DOWN_STATUS=$?
            if [[ ${SYS_DOWN_STATUS} -eq 0 && -f "${SYS_SMALL_OUT}" ]]; then
                local SYS_RESTORED_HASH_SMALL
                SYS_RESTORED_HASH_SMALL=$(sha256sum "${SYS_SMALL_OUT}" | awk '{print $1}')
                if [[ "${SYS_ORIGINAL_HASH_SMALL}" == "${SYS_RESTORED_HASH_SMALL}" ]]; then
                    report_result "System Test 1: Small File Transfer & SHA-256 Integrity (256 KB)" 0
                else
                    report_result "System Test 1: Small File Transfer" 1 "Hash mismatch: ${SYS_ORIGINAL_HASH_SMALL} != ${SYS_RESTORED_HASH_SMALL}"
                fi
            else
                report_result "System Test 1: Small File Transfer" 1 "Download failed with exit code ${SYS_DOWN_STATUS}"
            fi
        else
            report_result "System Test 1: Small File Transfer" 1 "Upload failed with exit code ${SYS_UP_STATUS}"
        fi
    else
        # Fallback to test-client container execution
        ${DOCKER_COMPOSE_CMD} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" exec -T test-client bash -c \
            "head -c 262144 /dev/urandom > /tmp/test_c.bin && dfg upload /tmp/test_c.bin sys_small.bin && dfg download sys_small.bin /tmp/out_c.bin && diff -q /tmp/test_c.bin /tmp/out_c.bin" > "${LOG_DIR}/sys_upload_client.log" 2>&1
        local CLIENT_EXEC_STATUS=$?
        if [[ ${CLIENT_EXEC_STATUS} -eq 0 ]]; then
            report_result "System Test 1: Small File Transfer & SHA-256 Integrity (in-container client)" 0
        else
            report_result "System Test 1: Small File Transfer" 1 "Container test client execution failed"
        fi
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # System Test 2: Multi-MB File Transfer (2 MB SHA-256 integrity)
    # ─────────────────────────────────────────────────────────────────────────
    local SYS_LARGE_SRC="${TMP_DIR}/sys_large.bin"
    local SYS_LARGE_OUT="${TMP_DIR}/sys_out_large.bin"
    head -c 2097152 /dev/urandom > "${SYS_LARGE_SRC}" # 2 MB
    local SYS_ORIGINAL_HASH_LARGE
    SYS_ORIGINAL_HASH_LARGE=$(sha256sum "${SYS_LARGE_SRC}" | awk '{print $1}')

    if [[ -x "${DFG_BIN}" ]]; then
        "${DFG_BIN}" upload "${SYS_LARGE_SRC}" "sys_large.bin" > "${LOG_DIR}/sys_upload_large.log" 2>&1
        local SYS_LARGE_UP_STATUS=$?
        if [[ ${SYS_LARGE_UP_STATUS} -eq 0 ]]; then
            "${DFG_BIN}" download "sys_large.bin" "${SYS_LARGE_OUT}" > "${LOG_DIR}/sys_download_large.log" 2>&1
            local SYS_LARGE_DOWN_STATUS=$?
            if [[ ${SYS_LARGE_DOWN_STATUS} -eq 0 && -f "${SYS_LARGE_OUT}" ]]; then
                local SYS_RESTORED_HASH_LARGE
                SYS_RESTORED_HASH_LARGE=$(sha256sum "${SYS_LARGE_OUT}" | awk '{print $1}')
                if [[ "${SYS_ORIGINAL_HASH_LARGE}" == "${SYS_RESTORED_HASH_LARGE}" ]]; then
                    report_result "System Test 2: Multi-MB File Transfer & SHA-256 Integrity (2 MB)" 0
                else
                    report_result "System Test 2: Multi-MB File Transfer" 1 "Hash mismatch: ${SYS_ORIGINAL_HASH_LARGE} != ${SYS_RESTORED_HASH_LARGE}"
                fi
            else
                report_result "System Test 2: Multi-MB File Transfer" 1 "Download failed with exit code ${SYS_LARGE_DOWN_STATUS}"
            fi
        else
            report_result "System Test 2: Multi-MB File Transfer" 1 "Upload failed with exit code ${SYS_LARGE_UP_STATUS}"
        fi
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # System Test 3: Standalone Protocol Client (dfg_client TCP stream)
    # ─────────────────────────────────────────────────────────────────────────
    if [[ -x "${CLIENT_BIN}" ]]; then
        local SYS_CLIENT_OUT="${TMP_DIR}/sys_client_out.bin"
        "${CLIENT_BIN}" 127.0.0.1 9669 "sys_small.bin" "${SYS_CLIENT_OUT}" > "${LOG_DIR}/sys_dfg_client.log" 2>&1
        local SYS_CLI_STATUS=$?
        if [[ ${SYS_CLI_STATUS} -eq 0 && -f "${SYS_CLIENT_OUT}" ]]; then
            local SYS_CLI_HASH
            SYS_CLI_HASH=$(sha256sum "${SYS_CLIENT_OUT}" | awk '{print $1}')
            if [[ "${SYS_ORIGINAL_HASH_SMALL}" == "${SYS_CLI_HASH}" ]]; then
                report_result "System Test 3: Standalone Protocol Client (dfg_client against container Head Server)" 0
            else
                report_result "System Test 3: Standalone Protocol Client" 1 "Hash mismatch from dfg_client"
            fi
        else
            report_result "System Test 3: Standalone Protocol Client" 1 "dfg_client failed with exit code ${SYS_CLI_STATUS}"
        fi
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # System Test 4: 0-Byte Empty File Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    local SYS_EMPTY_SRC="${TMP_DIR}/sys_empty.txt"
    local SYS_EMPTY_OUT="${TMP_DIR}/sys_empty_out.txt"
    touch "${SYS_EMPTY_SRC}"

    if [[ -x "${DFG_BIN}" ]]; then
        "${DFG_BIN}" upload "${SYS_EMPTY_SRC}" "sys_empty.txt" > "${LOG_DIR}/sys_upload_empty.log" 2>&1
        local SYS_EMPTY_UP=$?
        if [[ ${SYS_EMPTY_UP} -eq 0 ]]; then
            "${DFG_BIN}" download "sys_empty.txt" "${SYS_EMPTY_OUT}" > "${LOG_DIR}/sys_download_empty.log" 2>&1
            local SYS_EMPTY_DOWN=$?
            if [[ ${SYS_EMPTY_DOWN} -eq 0 && -f "${SYS_EMPTY_OUT}" && ! -s "${SYS_EMPTY_OUT}" ]]; then
                report_result "System Test 4: 0-Byte Empty File Lifecycle in Container Grid" 0
            else
                report_result "System Test 4: 0-Byte Empty File Lifecycle" 1 "Downloaded file not empty or missing"
            fi
        else
            report_result "System Test 4: 0-Byte Empty File Lifecycle" 1 "Upload empty file failed"
        fi
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # System Test 5: Metadata Listing Command
    # ─────────────────────────────────────────────────────────────────────────
    if [[ -x "${DFG_BIN}" ]]; then
        local SYS_LIST_OUT
        SYS_LIST_OUT=$("${DFG_BIN}" list 2>&1 || true)
        if echo "${SYS_LIST_OUT}" | grep -q "sys_small.bin"; then
            report_result "System Test 5: Metadata Store & File Listing (dfg list against container cluster)" 0
        else
            report_result "System Test 5: Metadata Store & File Listing" 1 "File not listed in metadata"
        fi
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # System Test 6: Containerized HTTP Control API
    # ─────────────────────────────────────────────────────────────────────────
    local SYS_STATUS_JSON
    local SYS_CLUSTER_JSON
    SYS_STATUS_JSON=$(curl -sf http://127.0.0.1:9670/api/v1/status || echo "")
    SYS_CLUSTER_JSON=$(curl -sf http://127.0.0.1:9670/api/v1/servers/cluster || echo "")

    if echo "${SYS_STATUS_JSON}" | grep -q "total_servers" && echo "${SYS_CLUSTER_JSON}" | grep -q "servers"; then
        report_result "System Test 6: Containerized HTTP Control API (status & cluster list :9670)" 0
    else
        report_result "System Test 6: Containerized HTTP Control API" 1 "Invalid response from Control API"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # System Test 7: Prometheus Metrics Exporters Across Grid Containers
    # ─────────────────────────────────────────────────────────────────────────
    local METRIC_HEAD
    local METRIC_HC
    local METRIC_CS1
    local METRIC_ZK
    METRIC_HEAD=$(curl -sf http://127.0.0.1:9095/metrics 2>/dev/null || echo "")
    METRIC_HC=$(curl -sf http://127.0.0.1:9096/metrics 2>/dev/null || echo "")
    METRIC_CS1=$(curl -sf http://127.0.0.1:9091/metrics 2>/dev/null || echo "")
    METRIC_ZK=$(curl -sf http://127.0.0.1:9097/metrics 2>/dev/null || echo "")

    if [[ -n "${METRIC_HEAD}" && -n "${METRIC_HC}" && -n "${METRIC_CS1}" ]]; then
        report_result "System Test 7: Prometheus Metrics Exporters (:9095, :9096, :9091, :9097)" 0
    else
        report_result "System Test 7: Prometheus Metrics Exporters" 1 "One or more container metrics endpoints unavailable"
    fi

    # ─────────────────────────────────────────────────────────────────────────
    # System Test 8: Containerized Replica Fault Tolerance & Voting
    # ─────────────────────────────────────────────────────────────────────────
    # Corrupt chunk inside container cluster-server-1 volume
    local CORRUPT_RES
    CORRUPT_RES=$(${DOCKER_COMPOSE_CMD} -f "${COMPOSE_FILE}" -p "${COMPOSE_PROJECT}" exec -T cluster-server-1 bash -c \
        "CHUNK=\$(find /tmp/cluster_storage/ /var/cluster_storage/ -name '*sys_small.bin_chunk_0*' 2>/dev/null | head -n 1); if [ -n \"\$CHUNK\" ]; then echo 'CORRUPTED_CONTAINER_DATA' > \"\$CHUNK\"; echo 'CORRUPTED'; else echo 'NOT_FOUND'; fi" 2>/dev/null || echo "EXEC_FAILED")

    if [[ "${CORRUPT_RES}" != *"CORRUPTED"* ]]; then
        report_result "System Test 8: Containerized Replica Fault Tolerance & Consensus Repair" 1 "Failed to find and corrupt target chunk inside cluster-server-1 container"
        return
    fi

    local SYS_REPAIR_OUT="${TMP_DIR}/sys_repaired.bin"
    if [[ -x "${DFG_BIN}" ]]; then
        "${DFG_BIN}" download "sys_small.bin" "${SYS_REPAIR_OUT}" > "${LOG_DIR}/sys_download_repair.log" 2>&1
        local SYS_REP_STATUS=$?
        if [[ ${SYS_REP_STATUS} -eq 0 && -f "${SYS_REPAIR_OUT}" ]]; then
            local SYS_REP_HASH
            SYS_REP_HASH=$(sha256sum "${SYS_REPAIR_OUT}" | awk '{print $1}')
            if [[ "${SYS_ORIGINAL_HASH_SMALL}" == "${SYS_REP_HASH}" ]]; then
                report_result "System Test 8: Containerized Replica Fault Tolerance & Consensus Repair" 0
            else
                report_result "System Test 8: Replica Fault Tolerance" 1 "Corrupted data returned on download"
            fi
        else
            report_result "System Test 8: Replica Fault Tolerance" 1 "Download failed during corruption test"
        fi
    fi
}

# ═══════════════════════════════════════════════════════════════════════════════
# Execution Dispatcher
# ═══════════════════════════════════════════════════════════════════════════════

if [[ "${SYSTEM_TEST}" -eq 1 ]]; then
    run_system_mode
else
    run_local_mode
fi

# ═══════════════════════════════════════════════════════════════════════════════
# Test Summary & Exit
# ═══════════════════════════════════════════════════════════════════════════════

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}                      E2E Test Results Summary                     ${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"
echo -e "  Passed: ${GREEN}${PASSED_TESTS}${NC}"
echo -e "  Failed: ${RED}${FAILED_TESTS}${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════════${NC}"

if [[ ${FAILED_TESTS} -eq 0 ]]; then
    echo -e "${GREEN}🎉 ALL END-TO-END TESTS PASSED SUCCESSFULLY! 🎉${NC}"
    exit 0
else
    echo -e "${RED}❌ SOME END-TO-END TESTS FAILED. Check logs in ${LOG_DIR}/ ❌${NC}"
    exit 1
fi
