# Set up development environment for Cloudberry Database on Linux

This document shares how to set up the development environments on different Linux systems (CentOS 7, RHEL, and Ubuntu). Before you start to [explore the Cloudberry Database](https://github.com/cloudberrydb/cloudberrydb/tree/main/readmes), follow this document to download and install the required tools and dependencies, and create demo user.

Take the following steps to set up the development environments:

1. [Clone GitHub repo](#step-1-clone-github-repo).
2. [Install dependencies](#step-2-install-dependencies).
3. [Perform prerequisite platform tasks](#step-3-prerequisite-platform-tasks).

## Step 1. Clone GitHub repo

Clone the GitHub repository `cloudberrydb/cloudberrydb` to the target machine using `git clone git@github.com:cloudberrydb/cloudberrydb.git`.

## Step 2. Install dependencies

Install dependencies according to your operating systems:

- [For CentOS 7](#for-centos-7)
- [For RHEL 7 and 8](#for-rhel-7-and-8)
- [For Ubuntu 18.04 or later](#for-ubuntu-1804-or-later)

### For CentOS 7

The following dependencies work on CentOS 7. For other CentOS versions, these steps might work but are not guaranteed to work.

1. Run the Bash script `README.CentOS.bash` in the `readmes` directory of the `cloudberrydb/cloudberrydb` repository. To run this script, password is required. Then, some required dependencies will be automatically downloaded.

    ```bash
    ./README.CentOS.bash
    ```

2. Update the GNU Compiler Collection (GCC) to version `devtoolset-10` to support C++ 14.

    ```bash
    yum install centos-release-scl
    yum -y install devtoolset-10-gcc devtoolset-10-gcc-c++ devtoolset-10-binutils
    scl enable devtoolset-10 bash
    source /opt/rh/devtoolset-10/enable
    ```

3. Install additional packages required for some configurations.

    ```bash
    yum -y install R apr apr-devel apr-util automake autoconf bash bison bison-devel bzip2 bzip2-devel centos-release-scl curl flex flex-devel gcc gcc-c++ git gdb ibxml2 iproute krb5 krb5-devel less libcurl libcurl-devel libevent libevent-devel libxml2 libxml2-devel libyaml libzstd-devel libzstd.x86_64 make openldap openssh-client openssl openssl-devel openssl-libs perl python3-devel readline readline-devel rsync sed sudo tar vim wget which xerces-c-devel zip zip-devel zlib && \
    yum -y install epel-release.noarch && \
    yum -y install libzstd
    ```

4. Link cmake3 to cmake:

    ```bash
    sudo ln -sf /usr/bin/cmake3 /usr/local/bin/cmake
    ```

### For RHEL 7 and 8

1. Install Development Tools.

    - For RHEL 8: Install:

        ```bash
        sudo yum group install -y "Development Tools"
        ```

    - For RHEL versions (< 8.0):

        ```bash
        sudo yum-config-manager --enable rhui-REGION-rhel-server-rhscl
        sudo yum install -y devtoolset-7-toolchain
        ```

2. Install dependencies by running the `README.CentOS.bash` script.

    - For RHEL 8:

        ```bash
        # First install the epel extension for RHEL 8.
        # This makes sure that git and make is installed.
        sudo yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm
        
        # Then run the README.CentOS.bash script in the `readmes` directory.
        ./README.CentOS.bash
        ```

    - For RHEL 7:

        ```bash
        # First install the epel extension for RHEL 7.
        # This makes sure that git and make is installed.
        sudo yum install -y http://dl.fedoraproject.org/pub/epel/7/x86_64/e/epel-release-7-9.noarch.rpm
        
        # Then run the README.CentOS.bash script in the `readmes` directory.
        ./README.CentOS.bash
        ```

### For Ubuntu 18.04 or later

1. Install dependencies by running the `README.Ubuntu.bash` script in the `readmes` directory.

    ```shell
    ## You need to enter your password to run
    sudo ./README.Ubuntu.bash
    ```

   > **Note:**
   >
   > - When you run the `README.Ubuntu.bash` script for dependencies, you will be asked to configure `realm` for kerberos. You can enter any realm, because this is just for testing, and during testing, it will reconfigure a local server/client. If you want to skip this manual configuration, run `export DEBIAN_FRONTEND=noninteractive`.
   > - If you fail to download packages, we recommend that you can try another one software source for Ubuntu.

2. Install GCC 10. Ubuntu 18.04 and later versions should use GCC 10 or newer:

    ```bash
    ## Install gcc-10
    sudo apt install software-properties-common
    sudo add-apt-repository ppa:ubuntu-toolchain-r/test
    sudo apt install gcc-10 g++-10
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-10 100
    ```

## Step 3. Perform prerequisite platform tasks

After you have installed all the dependencies for your operating system, it is time to do some prerequisite platform tasks before you go on building Cloudberry Database. These operation include manually running `ldconfig` on all platforms, creating the `gpadmin` user, and setting up a password to start the Cloudberry Database and test.

1. Make sure that you add `/usr/local/lib` and `/usr/local/lib64` to the `/etc/ld.so.conf` file.

    ```bash
    $ vi /etc/ld.so.conf
    
    ## Add the following two lines to /etc/ld.so.conf
    /usr/local/lib
    /usr/local/lib64
    ```

   Then run the `ldconfig` command.

2. Create the `gpadmin` user and set up the SSH key.

   Manually create SSH keys based on different operating systems, so that you can run `ssh localhost` without a password.

    ```bash
    ### For CentOS 7
    useradd gpadmin  # Creates gpadmin user.
    chown -R gpadmin:gpadmin /cloudberrydb/
    su - gpadmin  # Uses the gpadmin user.
    ssh-keygen  # Creates SSH key.
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
    ```

    ```bash
    ### For Ubuntu
    useradd -r -m -s /bin/bash gpadmin  # Creates gpadmin user.
    chown -R gpadmin:gpadmin /cloudberrydb/
    su - gpadmin  # Uses the gpadmin user.
    ssh-keygen  # Creates SSH key.
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys 
    ```

3. Verify that you can use SSH to connect to your machine without a password.

    ```bash
    ssh gpadmin@localhost
    ```

   or

    ```bash
    ssh <hostname of your machine>  # e.g., ssh briarwood (You can use `hostname` to get the hostname of your machine.)
    ```

4. Set up your system configuration by following the installation guide on [README.md](https://github.com/cloudberrydb/cloudberrydb/tree/main/readmes).
