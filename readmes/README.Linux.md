# **Linux system environment deployment**
    This document mainly discusses the environment construction of Centos7, Rhel8, and Ubuntu related machines. Mainly through scripts and commands for downloading related dependencies, component upgrades, user creation, setting environment variables, and other operations.
### For CentOS:
    Only applicable to Centos7 version testing
1. Install Dependencies

  ```bash
  ./README.CentOS.bash
  ## Please enter the password according to the prompt：
  ```

2. Update gcc to (devtoolset-10) to support c++14

  ```bash
  yum install centos-release-scl
  yum -y install devtoolset-10-gcc devtoolset-10-gcc-c++ devtoolset-10-binutils
  scl enable devtoolset-10 bash
  source /opt/rh/devtoolset-10/enable
  ```

3. Install additional packages required for some configurations

  ```bash
  yum -y install R apr apr-devel apr-util automake autoconf bash bison bison-devel bzip2 bzip2-devel centos-release-scl curl flex flex-devel gcc gcc-c++ git gdb ibxml2 iproute krb5 krb5-devel less libcurl libcurl-devel libevent libevent-devel libxml2 libxml2-devel libyaml libzstd-devel libzstd.x86_64 make openldap openssh-client openssl openssl-devel openssl-libs perl python3-devel readline readline-devel rsync sed sudo tar vim wget which xerces-c-devel zip zip-devel zlib && \
  yum -y install epel-release.noarch && \
  yum -y install libzstd.x86_64 
  ```

4. Link cmake3 to cmake, run:

  ```bash
  sudo ln -sf /usr/bin/cmake3 /usr/local/bin/cmake
  ```

### For RHEL:

1. Install Development Tools.
  - For RHEL 8: Install `Development Tools`:

    ```bash
    sudo yum group install -y "Development Tools"
    ```

  - For RHEL versions (< 8.0): Install `devtoolset-7`:

    ```bash
    sudo yum-config-manager --enable rhui-REGION-rhel-server-rhscl
    sudo yum install -y devtoolset-7-toolchain
    ```

2. Install dependencies using README.CentOS.bash script.
  - For RHEL 8: Execute additional step before running README.CentOS.bash script.
  
    Note: Make sure installation of `Development Tools` includes `git` and `make` else install these tools manually.

    ```bash
    sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
    ```

  - Install dependencies using README.CentOS.bash script.

    ```bash
    ./README.CentOS.bash
    ## Please enter the password according to the prompt：
    ```

### For Ubuntu:

1. Install Dependencies
    When you run the README.Ubuntu.bash script for dependencies, you will be asked to configure realm for kerberos.<br/>You can enter any realm, since this is just for testing, and during testing, it will reconfigure a local server/client.<br/>If you want to skip this manual configuration, use: `export DEBIAN_FRONTEND=noninteractive`

  ```bash
  sudo ./README.Ubuntu.bash
  ## Please enter the password according to the prompt：
  ```
If the script download fails, you can try changing the software download source for Ubuntu based on your Ubuntu version

2. Ubuntu 18.04 and newer should have use gcc 10 or newer

  ```bash
  ## Installing gcc-10
  sudo apt install software-properties-common
  sudo add-apt-repository ppa:ubuntu-toolchain-r/test
  sudo apt install gcc-10 g++-10
  sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 100
  ```

### Common Platform Tasks:
    All operations after compilation and installation will be carried out under the gpadmin user, so we will manually execute ldconfig on all platforms, create the gpadmin user, and set up a password free connection to start the cloudberry collection and testing
1. Make sure that you add `/usr/local/lib` and `/usr/local/lib64` to `/etc/ld.so.conf`, then run command `ldconfig`.
   
  ```bash
  vi /etc/ld.so.conf
  ## Add the following content
  /usr/local/lib
  /usr/local/lib64
  
  ```
2. Create gpadmin and setup ssh keys
   Manually create ssh keys based on different operating systems, so that you can execute ssh localhost without a password e.g., 
   
   ```bash
   ### Centos7
   useradd gpadmin
   chown -R gpadmin:gpadmin cloudberrydb/
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

3. Verify that you can ssh to your machine name without a password.

   ```bash
   ssh gpadmin@localhost 
   or
   ssh <hostname of your machine>  # e.g., ssh briarwood (You can use `hostname` to get the hostname of your machine.)
   
   ```

4. Set up your system configuration by following the installation guide on [README.md](README.md)
