FROM cloudberrydb/cloudberrydb:centosbase_k8s_1.0
ARG PACKAGE
ARG BUILD_TYPE
LABEL description="dockerfile for cbdb k8s auto-deployment"
ADD ${PACKAGE} .
RUN yum -y install R apr apr-devel apr-util automake autoconf bash bison bison-devel bzip2 bzip2-devel centos-release-scl curl flex flex-devel gcc gcc-c++ git gdb ibxml2 iproute krb5 krb5-devel less libcurl libcurl-devel libevent libevent-devel libxml2 libxml2-devel libyaml libzstd-devel libzstd.x86_64 make openldap openssh-client openssl openssl-devel openssl-libs perl python3-devel readline readline-devel rsync sed sudo tar vim wget which xerces-c-devel zip zip-devel zlib && \
    yum -y install epel-release.noarch && \
    yum -y install libzstd.x86_64 && \
    yum -y install devtoolset-9-gcc* && \
    wget https://cbdb-deps.s3.amazonaws.com/etcd-v3.3.25-linux-amd64.tar.gz && \
    tar zxvf etcd-v3.3.25-linux-amd64.tar.gz && \
    mv etcd-v3.3.25-linux-amd64/etcd /usr/local/bin/etcd && \
    mv etcd-v3.3.25-linux-amd64/etcdctl /usr/local/bin/etcdctl && \
    rm -rf etcd-v3.3.25-linux-amd64.tar.gz && \
    rpm -ivh ${PACKAGE} && \
    rm ${PACKAGE} && \
    chmod u+s /bin/ping && \
    mkdir -p /data0/data1/primary && \
    mkdir -p /data0/data2/primary && \
    mkdir -p /data0/data3/primary && \
    mkdir -p /data0/data/master
