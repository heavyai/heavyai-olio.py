.DEFAULT_GOAL=all

-include .env

start:
	docker run -d --name omnisci-dev --ipc=host \
		-p ${OMNISCI_DB_PORT}:6274 \
		-p ${OMNISCI_DB_PORT_HTTP}:6278 \
		omnisci/core-os-cpu:v5.0.1

start_gpu:
	docker run -d --name omnisci-dev --ipc=host \
		-p ${OMNISCI_DB_PORT}:6274 \
		-p ${OMNISCI_DB_PORT_HTTP}:6278 \
		omnisci/core-os-cuda:v5.0.1 \
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
	python setup.py build
	# pip install -e .

clean:
	python setup.py clean

test:
	pytest

all: build