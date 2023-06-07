FROM docker.artifactory.hashdata.xyz/docker/cbdb:devel-devtoolset-10-cbdb-docker-ubuntu-2019-20230103
LABEL description="dockerfile for cbdb project auto-deployment"
ENTRYPOINT service ssh start && /bin/bash /home/gpadmin/workspace/cbdb/deploy/docker/docker_deploy.sh
RUN apt-get install -y zip libzip-dev zipcmp zipmerge ziptool libjansson-dev autotools-dev autoconf && \
    wget https://artifactory.hashdata.xyz/artifactory/utility/etcd-v3.3.25-linux-amd64.tar.gz && \
    tar zxvf etcd-v3.3.25-linux-amd64.tar.gz && \
    mv etcd-v3.3.25-linux-amd64/etcd /usr/local/bin/etcd && \
    mv etcd-v3.3.25-linux-amd64/etcdctl /usr/local/bin/etcdctl && \
    rm -rf etcd-v3.3.25-linux-amd64.tar.gz && \
    wget https://artifactory.hashdata.xyz/artifactory/utility/jansson-2.13.1.tar.gz && \
    tar -xvf jansson-2.13.1.tar.gz && \
    rm -rf jansson-2.13.1.tar.gz && \
    cd jansson-2.13.1 && \
    ./configure --prefix=/usr --disable-static && \
    make && \
    make install && \
    cp -f /usr/lib/libjansson.so* /usr/lib64/
