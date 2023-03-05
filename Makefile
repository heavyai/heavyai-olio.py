SHELL = /bin/sh
.DEFAULT_GOAL=all

DB_CONTAINER = heavyai_test
PYTHON = 3.8
OMNISCI_VERSION = v6.4.2
# OMNISCI_VERSION = latest

-include .env

develop:
	pip install -e '.[dev]'
	pre-commit install

start:
	docker run -d --rm --name ${DB_CONTAINER} \
		--ipc=host \
		-p ${HEAVYAI_DB_PORT}:6274 \
		-p ${HEAVYAI_DB_PORT_HTTP}:6278 \
		heavyai/core-os-cpu:${HEAVYAI_VERSION} \
		/opt/heavyai/startheavy --non-interactive \
		--data /var/lib/heavyai/storage --config /var/lib/heavyai/heavy.conf \
		--enable-runtime-udf --enable-table-functions --allowed-import-paths='["/"]'
.PHONY: start

start.gpu:
	docker run -d --rm --name ${DB_CONTAINER} \
		--ipc=host \
		--gpus=0 \
		-p ${HEAVYAI_DB_PORT}:6274 \
		-p ${HEAVYAI_DB_PORT_HTTP}:6278 \
		heavyai/heavyai-ee-cuda:${HEAVYAI_VERSION} \
		/opt/heavyai/startheavy --non-interactive \
		--data /var/lib/heavyai/storage --config /var/lib/heavyai/heavy.conf \
		--enable-runtime-udf --enable-table-functions
.PHONY: start.gpu

stop:
	docker stop ${DB_CONTAINER}
.PHONY: stop

down:
	docker rm -f ${DB_CONTAINER}
.PHONY: down

install:
	pip install -e .
.PHONY: install

build:
	python setup.py build
	# pip install -e .
.PHONY: build

check:
	# pre-commit
	black .
	# flake8
 .PHONY: check

clean:
	python setup.py clean
.PHONY: clean

test:
	pytest
.PHONY: test

build_docker:
	$(MAKE) -C docker
.PHONY: build_docker

all: build
