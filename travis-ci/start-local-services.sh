#!/bin/sh

set -e

docker-compose -f travis-ci/docker-compose.yml up &

# Wait for the FTP server to be available
while ! curl --connect-timeout 5 -sS -o /dev/null ftp://testuser:testpass@127.0.0.1/README.md; do
  echo 'Waiting for FTP to start'
  sleep 2
done
echo 'FTP service is available'

# Wait for the HTTP server to be available
while ! curl --connect-timeout 5 -sS -o /dev/null http://127.0.0.1:3030/README.md; do
  echo 'Waiting for HTTP to start'
  sleep 2
done
echo 'HTTP service is available'

# Wait for the SFTP server to be available
chmod 0400 ./packages/test-data/keys/ssh_client_rsa_key
while ! sftp \
  -P 2222 \
  -i ./packages/test-data/keys/ssh_client_rsa_key \
  -o 'ConnectTimeout=5' \
  -o 'StrictHostKeyChecking=no' \
  -o 'UserKnownHostsFile=/dev/null' \
  -o "PreferredAuthentications=publickey" \
  user@127.0.0.1:/keys/ssh_client_rsa_key.pub /dev/null; do
  echo 'Waiting for SFTP to start'
  sleep 2
done
echo 'SFTP service is available'

# Wait for the Elasticsearch service to be available
while ! nc -z 127.0.0.1 9200; do
  echo 'Waiting for Elasticsearch to start'
  sleep 2
done
echo 'Elasticsearch service is started'

while ! curl --connect-timeout 5 -sS http://127.0.0.1:9200/_cluster/health | grep green > /dev/null 2>&1; do
  echo 'Waiting for Elasticsearch status to be green'
  sleep 2
done
echo 'Elasticsearch status is green'

# Lambda seems to be the last service that's started up by Localstack
while ! nc -z 127.0.0.1 4574; do
  echo 'Waiting for Localstack Lambda service to start'
  sleep 2
done
echo 'Localstack Lambda service is started'
