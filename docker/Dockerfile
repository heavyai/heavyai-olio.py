FROM rust:1.56.1 AS logscraper

RUN cargo install --git https://github.com/omnisci/log-scraper


FROM mambaorg/micromamba:0.17.0

# This is the username created in the base micromamba image. Setting this variable here for convenience.
ENV USERNAME=micromamba

USER root

RUN apt-get update && \
    apt-get install -y build-essential

COPY --from=logscraper /usr/local/cargo/bin/omnisci-log-scraper /usr/local/bin/omnisci-log-scraper

USER $USERNAME

COPY environment.yml /tmp/environment.yml
RUN micromamba install -y -n base -f /tmp/environment.yml && \
    micromamba clean --all --yes

USER root
RUN mkdir /src && chown $USERNAME:$USERNAME /src
USER $USERNAME

RUN mkdir -p /src/omnisci-olio.py
COPY --chown=$USERNAME:$USERNAME . /src/omnisci-olio.py/
RUN cd /src/omnisci-olio.py && \
    pip install -e .
