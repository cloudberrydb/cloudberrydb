#!/bin/bash

set -u -e

main() {
        local build_type
        build_type=$1
        mkdir build
        cd build
        cmake -D CMAKE_BUILD_TYPE=${build_type} ../gpos_src
        make -j4
        ctest -j4
}

main "$@"
