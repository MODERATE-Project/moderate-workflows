#!/bin/bash

set -e
set -x

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" -c "CREATE DATABASE keycloak;"
