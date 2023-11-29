# This is a vectorization executor extension for cloudberry database.

## Env
### Docker Mode

1.Run docker_start.sh
```
./docker_start.sh cbdb_local_path docker_path
```
such as:
```
./docker_start.sh /light/workspace/cbdb /home/gpadmin/work
```
2.source compile.
compile cbdb and arrow and install vectorization.
see source compile chapter.


## Source Compile

### Compile and run CBDB

```
CFLAGS=-O0 CXXFLAGS='-O0 -std=c++14'  CPPFLAGS='-DGP_SERIALIZATION_DEBUG=1' ./configure --prefix=/home/gpadmin/install/gpdb --enable-debug --enable-cassert --enable-orca --with-python --with-libxml  
make -j8
make -j8 install
make create-demo-cluster
```

### Compile Arrow

clone arrow
```
git clone -b hdw-6.0.1-centos7 git@code.hashdata.xyz:hashdata/arrow.git
```

compile arrow
```
cd arrow/cpp
mkdir build
cmake  -DARROW_BUILD_STATIC=OFF -DARROW_PYTHON=ON -DARROW_GANDIVA=ON -DARROW_COMPUTE=ON -DARROW_CSV=ON -DARROW_PARQUET=ON -DARROW_DATASET=ON -DARROW_FILESYSTEM=ON -DARROW_BUILD_UTILITIES=ON -DARROW_JSON=ON -DARROW_ORC=OFF -DARROW_JEMALLOC=OFF -DGARROW_DEBUG=ON -DCMAKE_BUILD_TYPE=debug -DGARROW_ARROW_CPP_BUILD_TYPE=debug ..
make -j8
sudo make -j8 install
```

切到arrow根目录
```
ninja
sudo ninja install
```

### Compile Vectorization

```
make 
make install
```

## Build

Get arrow and orc libs first:
```
$ ./dependencies.sh
```
The arrow dependencies track the newest commit of arrow repo. Sepecify
commit id by editing dependencies.sh.

Then build:

```
$ make install
```

# Regress test

## all test

```
make installcheck
```

## vectorization test

include old clickhouse and new clickhouse.

```
make vec_test
```
## icw test

### icw_parallel test

this is a pg test.

step1: set pg plan.

```
export PGOPTIONS='-c optimizer=off'
```

step2: run test
```
make icw_parallel_test
```

