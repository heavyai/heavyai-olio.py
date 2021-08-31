.DEFAULT_GOAL=all

-include .env

start:
	docker run -d --name omnisci-dev --ipc=host \
		-p ${OMNISCI_DB_PORT}:6274 \
		-p ${OMNISCI_DB_PORT_HTTP}:6278 \
		omnisci/core-os-cpu:v5.7.0

start_gpu:
	docker run -d --name omnisci-dev --ipc=host \
		-p ${OMNISCI_DB_PORT}:6274 \
		-p ${OMNISCI_DB_PORT_HTTP}:6278 \
		omnisci/core-os-cuda:v5.7.0 \
		/omnisci/startomnisci --non-interactive --data /omnisci-storage/data --config /omnisci-storage/omnisci.conf \
		--enable-runtime-udf

stop:
	docker stop omnisci-dev
	docker rm omnisci-dev

deps:
	conda install -y pytest

install:
	pip install -e .

init: test_init install deps
	# TODO this is incomplete

build:
	pip install -e .
	python setup.py build
.PHONY: build

clean:
	python setup.py clean
.PHONY: clean

test:
	pytest
.PHONY: test

all: build
