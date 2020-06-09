# Makefile for AirCan
PACKAGE_NAME := aircan
PACKAGE_DIRS := aircan
TESTS_DIR := tests
VERSION_FILE := VERSION

PYTEST_EXTRA_ARGS := --flake8 --isort --doctest-modules -rsxX -l --tb=short --strict --cov-report term-missing:skip-covered --cov=tests --cov-fail-under 100

SHELL := bash
PYTHON := python
PIP := pip
PIP_COMPILE := pip-compile
PYTEST := pytest
GIT := git

VERSION := $(shell cat $(VERSION_FILE))
SOURCE_FILES := $(shell find $(PACKAGE_DIRS) $(TESTS_DIR) -type f -name "*.py")

default: help

## Regenerate requirements files
requirements: dev-requirements.txt dev-requirements.in

## Run all tests
test: dev-requirements.txt
	$(PIP) install -r dev-requirements.txt -e .
	$(PYTEST) $(PYTEST_EXTRA_ARGS) $(TESTS_DIR)

.PHONY: test requirements

# Regenerate requirements.txt
requirementstxt: requirements.in
	$(PIP_COMPILE) --no-emit-index-url --output-file=requirements.txt requirements.in

dev-requirements.txt: dev-requirements.in
	$(PIP_COMPILE) --no-emit-index-url --output-file=dev-requirements.txt dev-requirements.in

# Help related variables and targets
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
RESET  := $(shell tput -Txterm sgr0)
TARGET_MAX_CHAR_NUM := 20

## Show help
help:
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk '/^[a-zA-Z\-_0-9]+:/ { \
	  helpMessage = match(lastLine, /^## (.*)/); \
	  if (helpMessage) { \
	    helpCommand = substr($$1, 0, index($$1, ":")-1); \
	    helpMessage = substr(lastLine, RSTART + 3, RLENGTH); \
	    printf "  ${YELLOW}%-$(TARGET_MAX_CHAR_NUM)s${RESET} ${GREEN}%s${RESET}\n", helpCommand, helpMessage; \
	  } \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)
