#!/bin/bash

set -u -e -x

main() {
  local build_type
  build_type=$1

  shift
  local numdeps
  numdeps=$#
  for ((i=1 ; i <= numdeps ; i++))
  do
    install_dependency "$1"
    shift
  done

  mkdir build
  cd build
  cmake -D CMAKE_BUILD_TYPE=${build_type} -D CMAKE_INSTALL_PREFIX=../install ../orca_src
  make -j$(nproc_wrapper)
  ctest -j$(nproc_wrapper)
  make install
}

install_dependency() {
  local dependency_name
  dependency_name=$1
  tar -xzf $dependency_name/$dependency_name.tar.gz -C /usr/local
}

nproc_wrapper() {
  if ! type nproc &> /dev/null; then
    echo 4
  else
    command nproc
  fi
}

main "$@"
