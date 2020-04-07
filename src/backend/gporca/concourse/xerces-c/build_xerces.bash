#!/bin/bash

set -u -e -x

main() {
        wget http://www.trieuvan.com/apache//xerces/c/3/sources/xerces-c-3.1.2.tar.gz
        tar -xzf xerces-c-3.1.2.tar.gz
        cd xerces-c-3.1.2
        patch -p1 < ../xerces_patch/patches/xerces-c-gpdb.patch
        ./configure --prefix=$PWD/../install
        make -j$(nproc_wrapper)
        make install
}

nproc_wrapper() {
        if ! type nproc &> /dev/null; then
                echo 4
        else
                command nproc
        fi
}

main "$@"
