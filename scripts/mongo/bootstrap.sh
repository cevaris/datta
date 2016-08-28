#!/usr/bin/env bash

set -x
echo 'db.createUser({ user: "test_user", pwd: "$EcrEt0$aucE", roles: [ ], });' | mongo test_db
