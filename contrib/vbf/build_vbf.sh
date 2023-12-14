#!/bin/bash

set -e        # exit on error

root_dir=$(pwd)
build_dir="build"
prefix_dir="/usr/local"
task_type="install"
version=$(./version.sh project)

die() {
    echo "$@" 1>&2 ; exit 1
}

arg () {
    echo "$1" | sed "s/^${2-[^=]*=}//" | sed "s/:/;/g"
}

usage() {
echo '
Usage: '"$0"' [<options>]
Options: [defaults in brackets after descriptions]
Configuration:
    --help                          print this message
    --prefix=PREFIX                 install files in tree rooted at PREFIX
    --task=TASK                     specify the task type: test|install|clean
'
    exit 1
}

function prepare_build {
  clean
  mkdir -p $build_dir
  (cd $build_dir && cmake $root_dir -DCMAKE_BUILD_TYPE=$1 -DCMAKE_INSTALL_PREFIX=$2 -DCMAKE_INSTALL_LIBDIR=lib)
}

function clean {
  if [ -d $build_dir ]; then
    rm -rf $build_dir
  fi
}

while test $# != 0; do
    case "$1" in
    --prefix=*) dir=`arg "$1"`
                prefix_dir="$dir";;
    --task=*) task=`arg "$1"`
                task_type="$task";;
    --help) usage ;;
    *) die "Unknown option: $1" ;;
    esac
    shift
done

case "$task_type" in
  test)
    prepare_build Debug $prefix_dir
    make -C $build_dir
    make -C $build_dir test
    ;;

  install)
    prepare_build RelWithDebInfo $prefix_dir
    make -C $build_dir
    make -C $build_dir install
    ;;

  clean)
    clean
    ;;

  *)
    die "Unknown task type: ${task_type}"
	;;
esac

exit 0
