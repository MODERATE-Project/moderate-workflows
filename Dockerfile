FROM python:3.10-bullseye
ENV PATH_CODE /code
ENV PKG_NAME moderate
WORKDIR ${PATH_CODE}
COPY . .
RUN pip install -U ./${PKG_NAME}