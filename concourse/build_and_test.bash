#!/bin/bash

set -u -e

main() {
        mkdir build
        cd build
        cmake -D CMAKE_BUILD_TYPE=Debug ../gpos_src
        make -j4
        ctest -j4
}

main "$@"
