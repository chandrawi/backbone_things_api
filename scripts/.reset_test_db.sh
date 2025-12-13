#!/bin/bash

BASE_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && dirname $PWD )
ENV_FILE="${BASE_DIR}/.env"
AUTH_MIGRATION_DIR="${BASE_DIR}/database/migrations/auth/"
RESOURCE_MIGRATION_DIR="${BASE_DIR}/database/migrations/resource"

set -o allexport
source <(sed -e "s/\r//" -e '/^#/d;/^\s*$/d' -e "s/'/'\\\''/g" -e "s/=\(.*\)/=\"\1\"/g" "${ENV_FILE}")
set +o allexport

sqlx migrate revert --source $AUTH_MIGRATION_DIR --database-url $DATABASE_URL_AUTH_TEST --target-version 0
sqlx migrate run --source $AUTH_MIGRATION_DIR --database-url $DATABASE_URL_AUTH_TEST
sqlx migrate revert --source $RESOURCE_MIGRATION_DIR --database-url $DATABASE_URL_RESOURCE_TEST --target-version 0
sqlx migrate run --source $RESOURCE_MIGRATION_DIR --database-url $DATABASE_URL_RESOURCE_TEST
