SHELL = /bin/sh
.DEFAULT_GOAL=all

-include ../project.env
-include ../.env

build:
	docker build \
		-f Dockerfile \
		-t ${OLIO_DKR_IMG} ..
.PHONY: build

freeze:
	docker run -t --rm --entrypoint '' ${OLIO_DKR_IMG} bash -c "micromamba -n base list" | tr -s ' ' | tr -d "\015" > requirements-freeze.txt
	docker run -t --rm --entrypoint '' ${OLIO_DKR_IMG} bash -c "pip freeze" | tr -s ' ' | tr -d "\015" >> requirements-freeze.txt
.PHONY: freeze

all: build freeze
.PHONY: all
