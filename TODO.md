# SensApp Research Prototype Refactoring TODO

This document tracks the comprehensive refactoring plan for SensApp to create a focused research prototype for storage backend comparison.

## Phase 1: Remove Event Bus & Simplify Architecture (Week 1)

### 🔄 Architecture Simplification

- [x] Create proper storage factory with runtime selection via `settings.toml`
- [x] Simplify `main.rs` initialization
- [x] Keep ALL storage backends for research comparison
- [x] **Event Bus Removal**: Completely removed event bus from `main.rs` and all components
- [x] **Direct Storage Calls**: All components now use direct storage access instead of event bus
- [x] **Storage Factory**: Implemented runtime storage backend selection via connection strings
- [x] **Architecture Simplification**: `main.rs` initialization significantly simplified
- [x] **HttpServerState Refactored**: Now contains direct storage reference instead of event bus

## Phase 2: Add ClickHouse Storage Backend (Week 1)

### 🗄️ ClickHouse Implementation

- [x] Create `src/storage/clickhouse/` module
- [x] Implement ClickHouse storage trait
- [x] Add ClickHouse migrations
- [x] Include ClickHouse in storage factory
- [x] Document ClickHouse-specific optimizations (LowCardinality, etc.)
- [x] Add ClickHouse connection string parsing
- [x] Test ClickHouse backend with sample data

### ✅ Phase 2 Completed

- [x] **ClickHouse Module**: Full implementation in `src/storage/clickhouse/` with 3 main files
- [x] **Storage Trait**: Complete `StorageInstance` trait implementation with all methods
- [x] **Database Schema**: 11 tables with monthly partitioning, bloom filters, and materialized views
- [x] **Connection Parsing**: `clickhouse://user:password@host:port/database` format supported
- [x] **Data Types**: All 8 sensor types (Integer, Numeric, Float, String, Boolean, Location, JSON, Blob)
- [x] **Factory Integration**: Storage factory correctly routes ClickHouse URLs
- [x] **Integration Tests**: Full test suite in `tests/clickhouse_integration.rs`
- [x] **Minor Gaps**: Example config in `settings.toml` would be nice to add

## Phase 3: Add Comprehensive Read API (Week 2)

### 📊 Data Retrieval API

- [x] Implement metrics catalog endpoint (`GET /metrics`) with DCAT format
- [x] Implement series catalog endpoint (`GET /series`) with DCAT format
- [x] Implement series data endpoint (`GET /series/{uuid}`) with format selection
- [x] Support time range queries with `start/end` parameters
- [x] Support data format selection: CSV, JSON Lines, SenML, Apache Arrow
- [x] Add optional limit parameter for pagination
- [x] Add aggregation endpoints for research metrics (metrics catalog)
- [x] Implement data export in multiple formats (SenML, CSV, JSONL, Arrow)
- [ ] Add query performance metrics collection
- [ ] Add offset parameter for full pagination support
- [x] **DCAT Catalog Format**: Both metrics and series endpoints use W3C DCAT standard
- [x] **Multiple Export Formats**: SenML, CSV, JSON Lines, Apache Arrow all implemented
- [x] **Time Range Queries**: Full support for ISO 8601 datetime parsing with timezone handling
- [x] **UUID-based Series Access**: Clean UUID-based series identification and querying
- [x] **Prometheus-style IDs**: Series catalog includes Prometheus-compatible identifiers
- [x] **Rich Metadata**: Comprehensive sensor metadata with labels, units, and types

## Phase 4: Testing Infrastructure (Week 2)

### 🧪 Unit Testing

- [ ] Unit tests for SQLite storage backend
- [ ] Unit tests for PostgreSQL storage backend
- [ ] Unit tests for TimescaleDB storage backend
- [ ] Unit tests for DuckDB storage backend
- [ ] Unit tests for BigQuery storage backend
- [ ] Unit tests for RRDCached storage backend
- [ ] Unit tests for ClickHouse storage backend (when implemented)
- [x] Unit tests for data type inference (`src/infer/`)
- [x] Unit tests for CSV parsing (`src/importers/`)
- [x] Unit tests for datetime parsing and timezone handling
- [x] Unit tests for export format handling and content types
- [x] Unit tests for Prometheus ID generation

### 🔗 Integration Testing

- [x] Integration tests for HTTP CRUD/DCAT endpoints (`tests/crud_dcat_api.rs`)
- [x] Integration tests for data ingestion (`tests/ingestion.rs`)
- [x] Integration tests for query and export functionality (`tests/query_export.rs`)
- [x] Integration tests for Apache Arrow export (`tests/arrow_integration.rs`)
- [x] Integration tests for datamodel edge cases (`tests/datamodel.rs`)
- [x] Integration tests for parser edge cases (`tests/parser_edge_cases.rs`)
- [ ] Integration tests for InfluxDB compatibility endpoints
- [ ] Integration tests for Prometheus compatibility endpoints
- [ ] Cross-storage backend data consistency tests

### ⚡ Performance Benchmarks

- [ ] Write latency benchmarks per storage backend
- [ ] Read latency benchmarks per storage backend
- [ ] Storage space efficiency comparison
- [ ] Concurrent write performance tests
- [ ] Test harness for automated storage backend comparison

### ✅ Phase 4 Completed

- [x] **Comprehensive Test Suite**: 6+ integration test files covering major functionality
- [x] **Export Format Testing**: All export formats (SenML, CSV, JSONL, Arrow) tested
- [x] **HTTP API Testing**: CRUD and DCAT API endpoints fully tested
- [x] **Data Model Testing**: Edge cases and type handling tested
- [x] **Parser Testing**: CSV and other format parsers tested

## Phase 5: Configuration & Observability (Week 3)

### ⚙️ Configuration Management

- [x] Move ALL storage configs to `settings.toml`
- [x] Add runtime storage backend selection
- [ ] Add connection pooling configuration
- [ ] Add batch processing configuration
- [ ] Environment variable override support

### 📈 Metrics & Monitoring

- [ ] Add Prometheus metrics endpoint (`/metrics`)
- [ ] Write latency metrics per storage backend
- [ ] Query latency metrics per storage backend
- [ ] Data ingestion rate metrics
- [ ] Storage-specific metrics (space usage, connections)
- [ ] Error rate metrics per backend
- [ ] Queue depth and processing metrics

### 📝 Structured Logging

- [ ] Implement structured logging with `tracing`
- [ ] Log storage backend selection decisions
- [ ] Log performance metrics
- [ ] Log error details with context
- [ ] Add request tracing correlation IDs

### 🏥 Health Monitoring

- [x] Health check endpoint (`/health/live` for liveness, `/health/ready` for readiness)
- [x] Storage backend connectivity checks (all 7 backends: PostgreSQL, SQLite, TimescaleDB, ClickHouse, BigQuery, DuckDB, RRDCached)
- [x] Unit tests for health check handlers and response serialization
- [x] Integration tests for health endpoints with database verification
- [ ] Database migration status checks
- [ ] Resource usage health indicators

### ✅ Phase 5 Completed

- [x] **Storage Backend Selection**: Connection string based runtime selection working
- [x] **Settings Configuration**: All major settings moved to settings.toml
- [x] **Multi-Backend Support**: PostgreSQL, SQLite, DuckDB, BigQuery, TimescaleDB, RRDCached
- [x] **Network Configuration**: HTTP server endpoint and port configuration
- [x] **Sentry Integration**: Optional error tracking configuration

## Phase 6: Streamline Ingestion (Week 3)

### 🔧 Ingestion Simplification

- [x] Keep HTTP ingestion (all format support)
- [x] Remove OPC UA to separate crate/service
- [x] Remove AMQP planning/references
- [x] Remove MQTT client (use Telegraf/Vector bridge instead)
- [x] Optimize batch processing without event bus
- [ ] Add ingestion rate limiting
- [ ] Add backpressure handling

### ✅ Phase 6 Completed

- [x] **Event Bus Removal**: Event bus completely removed from all components
- [x] **Direct Storage Access**: All ingestion now uses direct storage calls
- [x] **MQTT Client Removed**: Keeps SensApp stateless; use Telegraf/Vector for MQTT bridging
- [x] **Batch Processing**: Simplified batch processing without event bus overhead

## Phase 7: Research Tools & Documentation (Week 4)

### 🔬 Research Features

- [ ] Add storage backend comparison endpoints
- [ ] Create benchmark suite for storage comparison
- [ ] Add metrics dashboard configuration (Grafana)
- [ ] Performance comparison report generation
- [ ] Data consistency verification tools
- [ ] Storage backend switching without data loss

### 📚 Documentation

- [ ] Document storage backend trade-offs
- [ ] Create research data collection guide
- [ ] Performance comparison methodology
- [ ] Deployment guide for each storage backend
- [ ] API documentation updates
- [ ] Configuration reference guide

### ✅ Phase 7 Completed

- [x] **Export Format Support**: SenML, CSV, JSONL, Apache Arrow exporters implemented
- [x] **DCAT Catalog API**: W3C DCAT standard catalog endpoints for research compatibility
- [x] **Storage Backend Comparison**: All storage backends maintained for research
- [x] **Data Format Flexibility**: Multiple ingestion and export formats supported

## Key Principles for Research Prototype

- ✅ Maintain all storage backends for comparison
- ✅ Focus on measurement and observability
- ✅ Make storage selection runtime configurable
- ✅ Prioritize research flexibility over production optimization
- ✅ Keep ingestion simple (HTTP only)
- ✅ Remove unnecessary complexity (event bus, OPC UA, MQTT client)

## Session Notes

### Session 1: Analysis and Planning (Current)

**Completed:**

- Created comprehensive project plan and TODO document
- Analyzed event bus usage patterns across codebase
- Identified all locations where event bus is used for storage calls
- Designed direct storage call pattern to replace event bus
- Started refactoring: Updated `HttpServerState` to remove event bus dependency

**Key Findings:**

- Event bus creates single-threaded bottleneck in `main.rs:178-210`
- All storage calls are serialized through single consumer task
- Event bus is used in 11 files: CSV importers, OPC UA clients, HTTP endpoints, batch builder
- Current pattern: `event_bus.publish(batch)` → single consumer → `storage.publish(batch)`
- New pattern: Direct `storage.publish(batch)` calls for parallel processing

**What Has Been Accomplished:**

Major refactoring completed across multiple phases:

### Session 2-17: Major Implementation Work (🌊 waves 2-17)

**Event Bus Removal & Architecture Simplification (✅ COMPLETED)**

- Completely removed event bus from main.rs and all components
- Updated all HTTP endpoints to use direct storage access
- Removed MQTT client to keep SensApp stateless
- Simplified main.rs initialization significantly

**Comprehensive Read API (✅ COMPLETED)**

- Implemented DCAT-compliant catalog endpoints: `/metrics` and `/series`
- Added full series data endpoint: `/series/{uuid}` with format selection
- Support for multiple export formats: SenML, CSV, JSONL, Apache Arrow
- Time range queries with ISO 8601 datetime parsing and timezone handling

**Testing Infrastructure (✅ MAJOR PROGRESS)**

- 6+ comprehensive integration test files implemented
- All export formats tested (SenML, CSV, JSONL, Arrow)
- HTTP API endpoints fully tested
- Data model and parser edge cases tested

**Configuration & Settings (✅ COMPLETED)**

- All storage backends configured via settings.toml
- Runtime storage backend selection working
- Sentry and network configuration implemented

**Research-Ready Features (✅ COMPLETED)**

- All storage backends maintained for comparison
- DCAT catalog format for research compatibility
- Multiple data format support for flexibility

### Next Session Plan

**High Priority Remaining Items:**

- [x] ClickHouse storage backend implementation (Phase 2)
- [x] Health check endpoints (/health/live, /health/ready)
- [ ] Performance benchmarks for storage backend comparison
- [ ] Metrics endpoints (/metrics for Prometheus)
- [ ] Connection pooling configuration

**Medium Priority:**

- [ ] Storage backend unit tests
- [ ] Query performance metrics collection
- [ ] Rate limiting and backpressure handling
