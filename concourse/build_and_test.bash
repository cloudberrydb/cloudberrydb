#!/bin/bash

set -u -e -x

main() {
        local build_type
        build_type=$1
        mkdir build
        cd build
        cmake -D CMAKE_BUILD_TYPE=${build_type} -D CMAKE_INSTALL_PREFIX=../install ../gpos_src
        make -j$(nproc_wrapper)
        ctest -j$(nproc_wrapper)
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
