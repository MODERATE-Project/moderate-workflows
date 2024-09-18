FROM python:3.10-bullseye
ENV PATH_CODE=/code
ENV PATH_LOCAL_PKG=moderate

RUN apt-get update -y \
    && DEBIANFRONTEND=noninteractive apt-get install -y git-lfs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR ${PATH_CODE}/${PATH_LOCAL_PKG}
COPY ${PATH_LOCAL_PKG}/setup.py .
RUN pip install --no-cache-dir -U .
WORKDIR ${PATH_CODE}
COPY . .
RUN pip install -U ./${PATH_LOCAL_PKG}
