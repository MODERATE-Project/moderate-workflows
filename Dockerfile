FROM python:3.10-bullseye
ENV PATH_CODE /code
ENV PKG_NAME moderate

RUN apt-get update -y \
    && DEBIANFRONTEND=noninteractive apt-get install -y git-lfs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR ${PATH_CODE}
COPY . .
RUN pip install -U ./${PKG_NAME}