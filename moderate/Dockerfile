FROM python:3.10-bullseye

ENV PATH_CODE=/code

RUN apt-get update -y \
    && DEBIANFRONTEND=noninteractive apt-get install -y git-lfs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR ${PATH_CODE}
COPY setup.py .
RUN pip install --no-cache-dir -U .
COPY . .
RUN pip install -U .
