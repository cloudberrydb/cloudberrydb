## For CentOS:

- Install Dependencies

  ```bash
  ./README.CentOS.bash
  ```

- Update gcc to (devtoolset-10) to support c++14

  ```bash
  yum install centos-release-scl
  yum -y install devtoolset-10-gcc devtoolset-10-gcc-c++ devtoolset-10-binutils
  scl enable devtoolset-10 bash
  source /opt/rh/devtoolset-10/enable
  ```

- Install additional packages required for some configurations

  ```bash
  yum -y install R apr apr-devel apr-util automake autoconf bash bison bison-devel bzip2 bzip2-devel centos-release-scl curl flex flex-devel gcc gcc-c++ git gdb ibxml2 iproute krb5 krb5-devel less libcurl libcurl-devel libevent libevent-devel libxml2 libxml2-devel libyaml libzstd-devel libzstd.x86_64 make openldap openssh-client openssl openssl-devel openssl-libs perl python3-devel readline readline-devel rsync sed sudo tar vim wget which xerces-c-devel zip zip-devel zlib && \
  yum -y install epel-release.noarch && \
  yum -y install libzstd.x86_64 && \
  yum -y install devtoolset-9-gcc*
  ```
  
- If you want to link cmake3 to cmake, run:

  ```bash
  sudo ln -sf /usr/bin/cmake3 /usr/local/bin/cmake
  ```

- Make sure that you add `/usr/local/lib` and `/usr/local/lib64` to
`/etc/ld.so.conf`, then run command `ldconfig`.

## For RHEL:

- Install Development Tools.
  - For RHEL 8: Install `Development Tools`:

    ```bash
    sudo yum group install -y "Development Tools"
    ```

  - For RHEL versions (< 8.0): Install `devtoolset-7`:

    ```bash
    sudo yum-config-manager --enable rhui-REGION-rhel-server-rhscl
    sudo yum install -y devtoolset-7-toolchain
    ```
  
- Install dependencies using README.CentOS.bash script.
  - For RHEL 8: Execute additional step before running README.CentOS.bash script.
  
    Note: Make sure installation of `Development Tools` includes `git` and `make` else install these tools manually.

    ```bash
    sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
    ```

  - Install dependencies using README.CentOS.bash script.

    ```bash
    ./README.CentOS.bash
    ```

## For Ubuntu:

- Install Dependencies
  When you run the README.Ubuntu.bash script for dependencies, you will be asked to configure realm for kerberos.
  You can enter any realm, since this is just for testing, and during testing, it will reconfigure a local server/client.
  If you want to skip this manual configuration, use:
  `export DEBIAN_FRONTEND=noninteractive`

  ```bash
  sudo ./README.Ubuntu.bash
  ```

- If the script download fails, you can try changing the software download source for Ubuntu

    ```bash
    cp /etc/apt/sources.list /etc/apt/sources.list.bak_yyyymmdd
    vim /etc/apt/sources.list #Open the sources.list file
    
    #Add the following content to the end of the file
    deb http://mirrors.aliyun.com/ubuntu/ bionic main restricted universe multiverse
    deb http://mirrors.aliyun.com/ubuntu/ bionic-security main restricted universe multiverse
    deb http://mirrors.aliyun.com/ubuntu/ bionic-updates main restricted universe multiverse
    deb http://mirrors.aliyun.com/ubuntu/ bionic-proposed main restricted universe multiverse
    deb http://mirrors.aliyun.com/ubuntu/ bionic-backports main restricted universe multiverse
    deb-src http://mirrors.aliyun.com/ubuntu/ bionic main restricted universe multiverse
    deb-src http://mirrors.aliyun.com/ubuntu/ bionic-security main restricted universe multiverse
    deb-src http://mirrors.aliyun.com/ubuntu/ bionic-updates main restricted universe multiverse
    deb-src http://mirrors.aliyun.com/ubuntu/ bionic-proposed main restricted universe multiverse
    deb-src http://mirrors.aliyun.com/ubuntu/ bionic-backports main restricted universe multiverse
    ```
- Ubuntu 18.04 and newer should have use gcc 7 or newer, but you can also enable gcc-7 on older versions of Ubuntu:

  ```bash
  ## Installing gcc-9
  sudo apt install software-properties-common
  sudo add-apt-repository ppa:ubuntu-toolchain-r/test
  sudo apt install gcc-9 g++-9
  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 100
  ```

## Common Platform Tasks:

Make sure that you add `/usr/local/lib` to `/etc/ld.so.conf`,
then run command `ldconfig`.
1. Create gpadmin and setup ssh keys
   Either use:

   ```bash
   # Requires cbdb clone to be named cbdb_src
   cbdb_src/concourse/scripts/setup_gpadmin_user.bash
   ```
   to create the gpadmin user and set up keys,

   OR

   Manually create ssh keys based on different operating systems, so that you can execute ssh localhost without a password e.g., 
   
   ```bash
   ### Centos7
   useradd gpadmin
   su - gpadmin 
   ssh-keygen
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   chmod 600 ~/.ssh/authorized_keys
   
   ### Ubuntu
   useradd -r -m -s /bin/bash gpadmin
   chown -R gpadmin:gpadmin cloudberrydb/
   su - gpadmin
   ssh-keygen
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   chmod 600 ~/.ssh/authorized_keys
 
   ```

1. Verify that you can ssh to your machine name without a password.

   ```bash
   ssh gpadmin@localhost 
   or
   ssh <hostname of your machine>  # e.g., ssh briarwood (You can use `hostname` to get the hostname of your machine.)
   
    ```

1. Set up your system configuration by following the installation guide on [docs.cloudberrydb.org](https://cloudberrydb.org/docs/cbdb-overview)


