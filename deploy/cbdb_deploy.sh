#!/bin/bash
set -eox

if [ $# -le 0 ]; then
    echo "Please specify the deploy action [build|run|test] env"
    exit 1
fi

deploy_action=${1}

user=${2}
if test -z "$user"
then
    user="gpadmin"
fi

home_dir=/home/${user}
mapped_dir=${home_dir}/workspace/cbdb
working_dir=${home_dir}/workspace/cbdb_dev
install_dir=${home_dir}/install

function cbdb_build() {
    #clean up obsoleted folder
    echo "[CBDB build] cleanup workspace..."

    if [ -d ${install_dir} ]
    then
      rm -rf ${install_dir}
    fi

    if [ -d ${working_dir} ]
    then
      rm -rf ${working_dir}
    fi

    #copy working directory
    echo "[CBDB build] init compile environment..."
    mkdir ${install_dir}
    mkdir ${working_dir}
    cp -rf ${mapped_dir}/* ${working_dir}/

    #init compilation environment params
    source ${working_dir}/deploy/cbdb_env.sh

    if [ -f ${working_dir}/README.md ]
    then
      echo "[CBDB build] working directory installed properly..."
    else
      echo "[CBDB build] working directory not installed properly and exit!"
      exit 1
    fi

    #chanign to working directory
    cd ${working_dir}

    #do compile configuration
    echo "[CBDB build] start to init configuraiton for code compile..."
    CFLAGS=-O0 CXXFLAGS='-O0 -std=c++14' ./configure --prefix=${install_dir}/cbdb --enable-debug --enable-cassert --enable-tap-tests --with-gssapi --with-libxml --with-quicklz --with-pythonsrc-ext --with-openssl

    #do compile
    echo "[CBDB build] start to compile binary file..."
    make -sj 12 install

    #start cluster
    source ~/install/cbdb/greenplum_path.sh
    export PGHOST=`hostname`
    cd ${working_dir}/gpAux/gpdemo && make

    #export preinstall parameters
    source ${working_dir}/deploy/cbdb_env.sh

    #download dbgen
    cd ~/workspace
    curl https://cbdb-deps.s3.amazonaws.com/dbgen.tar.gz -o dbgen.tar.gz
    tar xzf dbgen.tar.gz -C $GPHOME/bin
}

function cbdb_run() {
    if [ -f  "/tmp/.s.PGSQL.7000.lock" ]
    then
        echo "[CBDB run] cluser is already running..."
    else
        echo "[CBDB run] start to run cluster..."
        cd ${working_dir} && make create-demo-cluster
    fi
}

function cbdb_test() {
    echo "[CBDB test] start  to run test"
    cd ${working_dir} && make installcheck-world
}


case ${deploy_action} in
    build)
        cbdb_build
        ;;
     run)
        cbdb_run
        ;;
     test)
        cbdb_test
        ;;
esac
