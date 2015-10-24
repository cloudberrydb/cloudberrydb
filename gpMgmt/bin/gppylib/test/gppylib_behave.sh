#!/bin/bash

source $HOME/qa.sh

cd $BLDWRAP_TOP/src/gpMgmt/bin

make behave $1 TAR=tar 2>&1 
