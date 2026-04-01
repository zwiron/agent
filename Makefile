.DEFAULT_GOAL := help

## test: Run all unit tests
test:
	go test ./... -count=1 -v

## test-short: Run tests without integration tests
test-short:
	go test ./... -count=1 -v -short

## build: Build the agent binary
build:
	go build -o agent .

## tag: Fetch latest tag, bump, create and push.
##      Default bumps patch. Override: make tag BUMP=minor or BUMP=major
tag:
	@LATEST=$$(git tag -l 'v*' --sort=-v:refname | head -1); \
	if [ -z "$$LATEST" ]; then LATEST="v0.0.0"; fi; \
	MAJOR=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f1); \
	MINOR=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f2); \
	PATCH=$$(echo "$$LATEST" | sed 's/^v//' | cut -d. -f3); \
	BUMP=$(or $(BUMP),patch); \
	case "$$BUMP" in \
		major) MAJOR=$$((MAJOR + 1)); MINOR=0; PATCH=0 ;; \
		minor) MINOR=$$((MINOR + 1)); PATCH=0 ;; \
		patch) PATCH=$$((PATCH + 1)) ;; \
		*) echo "Invalid BUMP=$$BUMP (use major, minor, or patch)"; exit 1 ;; \
	esac; \
	NEXT="v$$MAJOR.$$MINOR.$$PATCH"; \
	echo "Latest tag: $$LATEST"; \
	echo "New tag:    $$NEXT"; \
	git tag -a "$$NEXT" -m "Release $$NEXT"; \
	git push origin "$$NEXT"; \
	echo "✓ Tagged and pushed $$NEXT"

## lint: Run linter
lint:
	golangci-lint run ./...

## tidy: Tidy all modules
tidy:
	go mod tidy

## help: Show this help
help:
	@echo "Usage:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':' | sed 's/^/  /'
