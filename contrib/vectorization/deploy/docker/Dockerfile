ARG base=amd64/ubuntu:20.04
FROM ${base}
ENV DEBIAN_FRONTEND noninteractive

ARG CBDB_DIR
ARG CONTAINER_DIR

# ali source
RUN sed -i s/archive.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g /etc/apt/sources.list \
    && sed -i s/security.ubuntu.com/mirrors.tuna.tsinghua.edu.cn/g /etc/apt/sources.list \
    && apt-get update && apt-get upgrade -y

########################################
#######  CBDB dep ######################
########################################

# cbdb dep
RUN apt-get update \
    && apt-get install -y \
    bison \
    ccache \
    curl \
    flex \
    git-core \
    gcc \
    g++ \
    inetutils-ping \
    krb5-kdc \
    krb5-admin-server \
    libapr1-dev \
    libbz2-dev \
    libcurl4-gnutls-dev \
    libevent-dev \
    libkrb5-dev \
    libpam-dev \
    sudo

RUN apt-get update \
    && apt-get install -y \
    libperl-dev \
    libreadline-dev \
    libssl-dev \
    libxerces-c-dev \
    libxml2-dev \
    libyaml-dev \
    libzstd-dev \
    locales \
    net-tools \
    ninja-build \
    openssh-client \
    openssh-server \
    openssl \
    pkg-config

RUN apt-get update \
    && apt-get install -y \
    python3-dev \
    python3-pip \
    python3-psutil \
    python3-pygresql \
    python3-yaml \
    zlib1g-dev \
    iproute2 \
    # devtools
    vim \
    gdb

# essential tools
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# user gpadmin
RUN useradd -m -s /bin/bash gpadmin && echo "gpadmin:gpadmin" | chpasswd && adduser gpadmin sudo
RUN su - gpadmin -c "ssh-keygen -t rsa -N '' -f /home/gpadmin/.ssh/id_rsa -q && cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys"

# run cbdb scripts
RUN mkdir -p /scripts
COPY README.Ubuntu.bash /scripts
WORKDIR /scripts
RUN chmod +x README.Ubuntu.bash
RUN ./README.Ubuntu.bash


########################################
#######  Arrow dep #####################
########################################

# cmake 3.18.2 for arrow
RUN curl -fsSL -O https://github.com/Kitware/CMake/releases/download/v3.18.2/cmake-3.18.2.tar.gz

# compile and install cmake
RUN tar -xzf cmake-3.18.2.tar.gz && cd cmake-3.18.2 && \
	 ./bootstrap && make -j8 && make -j8 install

# Installs LLVM toolchain, for Gandiva and testing other compilers
#
# Note that this is installed before the base packages to improve iteration
# while debugging package list with docker build.
ARG clang_tools=8
ARG llvm=13
RUN if [ "${llvm}" -gt "10" ]; then \
      apt-get update -y -q && \
      apt-get install -y -q --no-install-recommends \
          apt-transport-https \
          ca-certificates \
          gnupg \
          wget && \
      wget -O - https://apt.llvm.org/llvm-snapshot.gpg.key | apt-key add - && \
      echo "deb https://apt.llvm.org/focal/ llvm-toolchain-focal-${llvm} main" > \
         /etc/apt/sources.list.d/llvm.list && \
      if [ "${clang_tools}" != "${llvm}" -a "${clang_tools}" -gt 10 ]; then \
        echo "deb https://apt.llvm.org/focal/ llvm-toolchain-focal-${clang_tools} main" > \
           /etc/apt/sources.list.d/clang-tools.list; \
      fi \
    fi && \
    apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        clang-${clang_tools} \
        clang-${llvm} \
        clang-format-${clang_tools} \
        clang-tidy-${clang_tools} \
        llvm-${llvm}-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

# Installs C++ toolchain and dependencies
RUN apt-get update -y -q && \
    apt-get install -y -q --no-install-recommends \
        autoconf \
        ca-certificates \
        ccache \
        g++ \
        gcc \
        gdb \
        git \
        libbenchmark-dev \
        libboost-filesystem-dev \
        libboost-system-dev \
        libbrotli-dev \
        libbz2-dev \
        libc-ares-dev \
        libcurl4-openssl-dev \
        libgflags-dev \
        libgoogle-glog-dev \
        liblz4-dev \
        libprotobuf-dev \
        libprotoc-dev \
        libre2-dev \
        libsnappy-dev \
        libssl-dev \
        libthrift-dev \
        libutf8proc-dev \
        libzstd-dev \
        make \
        ninja-build \
        pkg-config \
        protobuf-compiler \
        python3-pip \
        rapidjson-dev \
        tzdata \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

RUN apt-get update -y -q && \
    apt-get install -y libglib2.0-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists*

RUN ln  -s  /usr/bin/python3   /usr/bin/python  && \
	pip3 install numpy -i https://pypi.doubanio.com/simple && \
	pip3 install meson -i https://pypi.doubanio.com/simple 


########################################
#######  Common conf ###################
########################################

RUN apt-get update \
    && apt-get install -y git \
    && apt-get install -y zsh \
    && apt-get install -y sudo \
    && git clone --depth 1 https://github.com/robbyrussell/oh-my-zsh.git ~/.oh-my-zsh \
    && cp ~/.oh-my-zsh/templates/zshrc.zsh-template ~/.zshrc \
    && echo "bindkey '^R' history-incremental-pattern-search-backward" >> ~/.zshrc \
    && chsh -s /bin/zsh \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY startup.sh /scripts
WORKDIR /scripts
RUN chmod +x startup.sh
WORKDIR /home/gpadmin
ENTRYPOINT service ssh start && /scripts/startup.sh gpadmin $CONTAINER_DIR
