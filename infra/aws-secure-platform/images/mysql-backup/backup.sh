#!/usr/bin/env bash

set -euo pipefail

: "${MYSQL_HOST:?MYSQL_HOST is required}"
: "${MYSQL_PORT:=3306}"
: "${MYSQL_DATABASE:?MYSQL_DATABASE is required}"
: "${MYSQL_USER:?MYSQL_USER is required}"
: "${MYSQL_PASSWORD:?MYSQL_PASSWORD is required}"
: "${S3_BUCKET_NAME:?S3_BUCKET_NAME is required}"
: "${S3_PREFIX:=mysql}"
: "${AWS_DEFAULT_REGION:?AWS_DEFAULT_REGION is required}"

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
object_key="${S3_PREFIX%/}/${MYSQL_DATABASE}/${timestamp}.sql.gz"
tmpfile="$(mktemp "/tmp/${MYSQL_DATABASE}-${timestamp}-XXXXXX.sql.gz")"

cleanup() {
  rm -f "${tmpfile}"
}

trap cleanup EXIT

export MYSQL_PWD="${MYSQL_PASSWORD}"

mysqldump \
  --host="${MYSQL_HOST}" \
  --port="${MYSQL_PORT}" \
  --user="${MYSQL_USER}" \
  ${MYSQLDUMP_EXTRA_ARGS:-} \
  "${MYSQL_DATABASE}" \
  | gzip -c > "${tmpfile}"

aws s3 cp \
  "${tmpfile}" \
  "s3://${S3_BUCKET_NAME}/${object_key}" \
  --region "${AWS_DEFAULT_REGION}" \
  --only-show-errors

echo "Uploaded backup to s3://${S3_BUCKET_NAME}/${object_key}"
