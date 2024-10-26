#!/bin/bash
set -e

# Wait for PostgreSQL to start
until pg_isready; do
  echo "Waiting for PostgreSQL to start..."
  sleep 1
done

# Modify pg_hba.conf to allow connections from all IP addresses
sed -i 's/host all all 127.0.0.1\/32 md5/host all all 0.0.0.0\/0 md5/' /var/lib/postgresql/data/pg_hba.conf

# Restart PostgreSQL to apply changes
pg_ctl restart