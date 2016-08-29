#!/usr/bin/env bash

set -x
cqlsh localhost  -u cassandra -p cassandra --execute "CREATE USER IF NOT EXISTS test_user WITH PASSWORD '\$EcrEt0\$aucE' SUPERUSER;"
