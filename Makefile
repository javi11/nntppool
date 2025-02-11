GO ?= go
TOOLS = $(CURDIR)/.tools

.DEFAULT_GOAL := check

$(TOOLS):
	@mkdir -p $@
$(TOOLS)/%: | $(TOOLS)
	@GOBIN=$(TOOLS) go install $(PACKAGE)

GOLANGCI_LINT = $(TOOLS)/golangci-lint
$(GOLANGCI_LINT): PACKAGE=github.com/golangci/golangci-lint/cmd/golangci-lint@latest

JUNIT = $(TOOLS)/junit
$(JUNIT): PACKAGE=github.com/jstemmer/go-junit-report/v2@latest

GOVULNCHECK = $(TOOLS)/govulncheck
$(GOVULNCHECK): PACKAGE=golang.org/x/vuln/cmd/govulncheck@latest

GOMOCKGEN = $(TOOLS)/mockgen
$(GOMOCKGEN): PACKAGE=go.uber.org/mock/mockgen@latest

.PHONY: generate
generate: tools
	go generate ./...

.PHONY: govulncheck
govulncheck: | $(GOVULNCHECK)
	@$(TOOLS)/govulncheck ./...

.PHONY: tidy go-mod-tidy
tidy: go-mod-tidy
go-mod-tidy:
	$(GO) mod tidy

.PHONY: tools
tools: $(GOLANGCI_LINT) $(JUNIT) $(GOVULNCHECK) $(GOMOCKGEN)

.PHONY: golangci-lint golangci-lint-fix
golangci-lint-fix: ARGS=--fix
golangci-lint-fix: golangci-lint
golangci-lint: | $(GOLANGCI_LINT)
	@$(TOOLS)/golangci-lint run $(ARGS)

.PHONY: junit
junit: | $(JUNIT)
	mkdir -p ./test-results && $(GO) test -v 2>&1 ./... | $(TOOLS)/go-junit-report -set-exit-code > ./test-results/report.xml

.PHONY: coverage
coverage:
	$(GO) test -v -coverprofile=coverage.out ./...

.PHONY: coverage-html
coverage-html: coverage
	$(GO) tool cover -html=coverage.out -o coverage.html

.PHONY: coverage-func
coverage-func: coverage
	$(GO) tool cover -func=coverage.out

.PHONY: lint
lint: go-mod-tidy golangci-lint

.PHONY: test test-race
test-race: ARGS=-race
test-race: test
test:
	$(GO) test $(ARGS) ./...

.PHONY: check
check: generate go-mod-tidy golangci-lint test-race

.PHONY: git-hooks
git-hooks:
	@echo '#!/bin/sh\nmake' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit