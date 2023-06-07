FROM docker.artifactory.hashdata.xyz/docker/cbdb:centosbase_k8s_1.0
ARG PACKAGE
ARG BUILD_TYPE
LABEL description="dockerfile for cbdb k8s auto-deployment"
RUN curl -O http://artifactory.hashdata.xyz/artifactory/hashdata-repository/intermediate-artifacts/centos7/x86_64/cbdb/${BUILD_TYPE}/${PACKAGE} && \
    yum -y install R apr apr-devel apr-util automake autoconf bash bison bison-devel bzip2 bzip2-devel centos-release-scl curl flex flex-devel gcc gcc-c++ git gdb ibxml2 iproute krb5 krb5-devel less libcurl libcurl-devel libevent libevent-devel libxml2 libxml2-devel libyaml libzstd-devel libzstd.x86_64 make openldap openssh-client openssl openssl-devel openssl-libs perl python3-devel readline readline-devel rsync sed sudo tar vim wget which xerces-c-devel zip zip-devel zlib && \
    yum -y install epel-release.noarch && \
    yum -y install libzstd.x86_64 && \
    yum -y install devtoolset-9-gcc* && \
    wget https://artifactory.hashdata.xyz/artifactory/utility/etcd-v3.3.25-linux-amd64.tar.gz && \
    tar zxvf etcd-v3.3.25-linux-amd64.tar.gz && \
    mv etcd-v3.3.25-linux-amd64/etcd /usr/local/bin/etcd && \
    mv etcd-v3.3.25-linux-amd64/etcdctl /usr/local/bin/etcdctl && \
    rm -rf etcd-v3.3.25-linux-amd64.tar.gz && \
    rpm -ivh ${PACKAGE} && \
    rm ${PACKAGE} && \
    wget https://artifactory.hashdata.xyz/artifactory/utility/jansson-2.13.1.tar.gz && \
    tar -xvf jansson-2.13.1.tar.gz && \
    rm -rf jansson-2.13.1.tar.gz && \
    cd jansson-2.13.1 && \
    ./configure --prefix=/usr --disable-static && \
    make && \
    make install && \
    cp -f /usr/lib/libjansson.so* /usr/lib64/ && \
    wget -O /root/cmake-3.25.1-linux-x86_64.tar.gz https://artifactory.hashdata.xyz/artifactory/utility/cmake-3.25.1-linux-x86_64.tar.gz && \
    mkdir -p /root/cmake-3.25.1-linux-x86_64 && \
    tar -xvf /root/cmake-3.25.1-linux-x86_64.tar.gz -C /root/cmake-3.25.1-linux-x86_64 && \
    ln -s /root/cmake-3.25.1-linux-x86_64/cmake-3.25.1-linux-x86_64/bin/cmake /usr/bin/cmake && \
    chmod u+s /bin/ping && \
    mkdir -p /data0/data1/primary && \
    mkdir -p /data0/data2/primary && \
    mkdir -p /data0/data3/primary && \
    mkdir -p /data0/data/master
