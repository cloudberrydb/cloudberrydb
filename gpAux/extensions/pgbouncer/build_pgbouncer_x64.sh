#!/bin/bash
set -e



CURDIR=$(cd $(dirname $0); pwd)
DEPSRC=$CURDIR/sources
PREFIX=$CURDIR/install

mkdir -p $DEPSRC

if [ ! -z $CLEANUP ]; then
rm -rf $DEPSRC/*
(cd packages && tar xzf libevent-2.0.22-stable.tar.gz -C $DEPSRC)
(cd packages && tar xzf openldap-2.4.40.tgz -C $DEPSRC)
(cd packages && tar xzf openssl-1.0.2c.tar.gz -C $DEPSRC)
(cd packages && tar xzf stunnel-5.19.tar.gz -C $DEPSRC)
fi

# openssl
cd $DEPSRC/openssl-1.0.2c
if [ ! -z $CLEANUP ]; then
./Configure linux-x86_64 --prefix=$PREFIX no-shared no-asm
fi
make
make install_sw

# libevent
cd $DEPSRC/libevent-2.0.22-stable
if [ ! -z $CLEANUP ]; then
./configure  --prefix=$PREFIX --disable-thread-support
fi
make 
make install

# ldap
cd $DEPSRC/openldap-2.4.40
if [ ! -z $CLEANUP ]; then
CFALGS="-I$PREFIX/include" LDFLAGS="-L$PREFIX/libs" ./configure --with-tls --disable-slapd --with-threads --prefix=$PREFIX  --disable-shared  --without-cyrus-sasl
fi
make 
cd $DEPSRC/openldap-2.4.40/libraries
make install

# pgbouncer
cd $CURDIR/pgbouncerdev
if [ ! -z $CLEANUP ]; then
./configure --prefix=$PREFIX --with-libevent=$PREFIX --enable-evdns
fi
make
make install


# stunnel
cd $DEPSRC/stunnel-5.19
if [ ! -z $CLEANUP ]; then
LDFLAGS="-Wl,-rpath,'"'$$'"ORIGIN/../lib' --static" ./configure --prefix=$PREFIX --with-ssl=$PREFIX
fi
make
make install
