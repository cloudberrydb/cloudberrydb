#!/bin/bash
cd `dirname "${BASH_SOURCE[0]}"`

source ./conf.sh

./ddl.sh

./load.sh
