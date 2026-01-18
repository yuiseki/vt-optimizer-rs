-include .env
export

.PHONY: help build test release clean fmt clippy run install check inspect-mbtiles inspect-pmtiles

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: ## Build the project in debug mode
	cargo build --verbose

release: ## Build the project in release mode
	cargo build --release --verbose

test: ## Run tests
	cargo test --verbose

check: ## Check the project for errors without building
	cargo check --verbose

fmt: ## Format the code
	cargo fmt --all

fmt-check: ## Check code formatting without making changes
	cargo fmt --all -- --check

clippy: ## Run clippy linter
	cargo clippy --all-targets --all-features -- -D warnings

bump-version: ## Bump package version (requires VERSION env, optional BUMP_BRANCH=1)
	@if [ -z "$(VERSION)" ]; then \
		echo "Error: VERSION is not set. Example: VERSION=0.2.0 make bump-version"; \
		exit 1; \
	fi
	@if [ "$(BUMP_BRANCH)" = "1" ]; then \
		branch="bump/v$(VERSION)"; \
		git switch -c "$$branch" 2>/dev/null || git switch "$$branch"; \
	fi
	@if command -v cargo-set-version >/dev/null 2>&1; then \
		cargo set-version "$(VERSION)"; \
	else \
		sed -i -E 's/^version = \".*\"/version = \"$(VERSION)\"/' Cargo.toml; \
	fi
	cargo check --quiet

clean: ## Clean build artifacts
	cargo clean
	rm -rf target/
	rm -rf tmp/*

run: ## Run the project
	cargo run

install: ## Install the binary to ~/.cargo/bin
	cargo install --path .

watch: ## Watch for changes and rebuild
	cargo watch -x build

dev: fmt clippy test ## Run formatter, clippy, and tests (development workflow)

ci: fmt-check clippy test ## Run CI checks locally

all: clean build test ## Clean, build, and test

inspect-mbtiles: ## Inspect MBTiles file (requires MBTILES_PATH in .env)
	@if [ -z "$(MBTILES_PATH)" ]; then \
		echo "Error: MBTILES_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	cargo run -- inspect $(MBTILES_PATH)

inspect-mbtiles-json: ## Inspect MBTiles file and output JSON (requires MBTILES_PATH in .env)
	@if [ -z "$(MBTILES_PATH)" ]; then \
		echo "Error: MBTILES_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	cargo run -- inspect $(MBTILES_PATH) --report-format json

optimize-mbtiles: ## Optimize MBTiles file (requires MBTILES_PATH in .env)
	@if [ -z "$(MBTILES_PATH)" ]; then \
		echo "Error: MBTILES_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	@if [ -z "$(OUTPUT_MBTILES_PATH)" ]; then \
		echo "Error: OUTPUT_MBTILES_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	@if [ -z "$(STYLE_PATH)" ]; then \
		echo "Error: STYLE_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	cargo run -- optimize $(MBTILES_PATH) --style $(STYLE_PATH) --output $(OUTPUT_MBTILES_PATH)

optimize-mbtiles-json: ## Optimize MBTiles file and output JSON report (requires MBTILES_PATH in .env)
	@if [ -z "$(MBTILES_PATH)" ]; then \
		echo "Error: MBTILES_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	@if [ -z "$(OUTPUT_MBTILES_PATH)" ]; then \
		echo "Error: OUTPUT_MBTILES_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	@if [ -z "$(STYLE_PATH)" ]; then \
		echo "Error: STYLE_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	cargo run -- optimize $(MBTILES_PATH) --style $(STYLE_PATH) --output $(OUTPUT_MBTILES_PATH) --report-format json

inspect-pmtiles: ## Inspect PMTiles file (requires PMTILES_PATH in .env)
	@if [ -z "$(PMTILES_PATH)" ]; then \
		echo "Error: PMTILES_PATH is not set. Please set it in .env file"; \
		exit 1; \
	fi
	cargo run -- inspect $(PMTILES_PATH)

.DEFAULT_GOAL := help
