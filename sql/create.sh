#!/bin/sh

DB_USER=$1
DB_PASS=$2

echo Creating database
mysql -u${DB_USER} -p${DB_PASS} --execute="source create_mysql_objects.sql";

