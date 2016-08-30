#!/usr/bin/env bash

set -x
cqlsh localhost  -u cassandra -p cassandra --execute "CREATE USER IF NOT EXISTS test_user WITH PASSWORD '\$EcrEt0\$aucE' SUPERUSER;"
cqlsh localhost  -u cassandra -p cassandra --execute "CREATE KEYSPACE IF NOT EXISTS test_db WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };"


