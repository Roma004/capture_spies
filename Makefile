FORMAT_DIRS=backend backend.py migrations
LINTER_DIRS=backend backend.py migrations

.PHONY: install
install:
	python -m pip install -r requirements.txt

.PHONY: pretty
pretty: isort autoflake black

.PHONY: black
black:
	python -m black -t py310 $(FORMAT_DIRS)

.PHONY: isort
isort:
	python -m isort $(FORMAT_DIRS)

.PHONY: autoflake
autoflake:
	python -m autoflake -i -r --ignore-init-module-imports --remove-all-unused-imports --expand-star-imports $(FORMAT_DIRS)

.PHONY: mypy
mypy:
	mypy $(LINTER_DIRS)

run-dev: SHELL=/bin/bash

.PHONY: run-dev
run-dev:
	export $$(grep -v '^#' ./.env | xargs)
	echo "$$HMAC_SECRET_KEY"
# python main.py
# $(SHELL) -c `export $$(grep -v '^#' ./.env | xargs) && echo $$SERVICES`
