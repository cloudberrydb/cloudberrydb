#!/bin/bash
PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source $PWD/conf.sh

test_file="$PWD/sql/valgrind.sql"

command="psql -d $DB -f $test_file"
valgrind_check
eval "$command"
