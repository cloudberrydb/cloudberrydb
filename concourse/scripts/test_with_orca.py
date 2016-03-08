#!/usr/bin/python2

import optparse
import os
import subprocess
import sys

def num_cpus():
    # Use multiprocessing module, available in Python 2.6+
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError, NotImplementedError):
        pass

    # Get POSIX system config value for number of processors.
    posix_num_cpus = os.sysconf("SC_NPROCESSORS_ONLN")
    if posix_num_cpus != -1:
        return posix_num_cpus

    # Guess
    return 2

def yum_install(pkg):
    return subprocess.call(["yum", "install", "-y", pkg])

def install_system_deps():
    status = subprocess.call(["yum", "--exclude=systemtap", "groupinstall", "-y",
                              "Development tools"])
    if status:
        return status

    status = subprocess.call(["yum", "install", "-y",
                              "sudo",
                              "passwd",
                              "openssh-server",
                              "ed",
                              "readline-devel",
                              "zlib-devel",
                              "curl-devel",
                              "bzip2-devel",
                              "python-devel",
                              "apr-devel",
                              "libevent-devel",
                              "openssl-libs",
                              "openssl-devel",
                              "libyaml",
                              "libyaml-devel",
                              "epel-release",
                              "htop",
                              "perl-Env",
                              "perl-ExtUtils-Embed",
                              "libxml2-devel",
                              "libxslt-devel"])
    if status:
        return status

    status = subprocess.call(["curl", "https://bootstrap.pypa.io/get-pip.py", "-o", "get-pip.py"])
    if status:
        return status

    status = subprocess.call(["python", "get-pip.py"])
    if status:
        return status

    status = subprocess.call(["pip", "install", "psutil", "lockfile", "paramiko", "setuptools",
                              "epydoc"])
    return status

def install_dependency(dependency_name):
    return subprocess.call(
        ["tar",
         "-xzf",
         dependency_name + "/" + dependency_name + ".tar.gz",
         "-C",
         "/usr/local"])

def install_gpdb(dependency_name):
    status = subprocess.call("mkdir -p /usr/local/gpdb", shell=True)
    if status:
        return status
    status = subprocess.call(
        ["tar",
         "-xzf",
         dependency_name + "/" + dependency_name + ".tar.gz",
         "-C",
         "/usr/local/gpdb"])
    return status

def configure():
    return subprocess.call(["./configure",
                            "--enable-orca",
                            "--enable-mapreduce",
                            "--with-perl",
                            "--with-libxml",
                            "--with-python",
                            "--prefix=/usr/local/gpdb"], cwd="gpdb_src")

def create_gpadmin_user():
    status = subprocess.call("gpdb_src/concourse/scripts/setup_gpadmin_user.bash")
    if status:
        return status

def icg():
    status = subprocess.call(
        "printf '\nLD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib\nexport \
        LD_LIBRARY_PATH' >> /usr/local/gpdb/greenplum_path.sh", shell=True)
    if status:
        return status
    status = subprocess.call([
        "runuser gpadmin -c \"source /usr/local/gpdb/greenplum_path.sh \
        && make cluster\""], cwd="gpdb_src/gpAux/gpdemo", shell=True)
    if status:
        return status
    return subprocess.call([
        "runuser gpadmin -c \"source /usr/local/gpdb/greenplum_path.sh \
        && source gpAux/gpdemo/gpdemo-env.sh && PGOPTIONS='-c optimizer=on' \
        make installcheck-good\""], cwd="gpdb_src", shell=True)

def main():
    parser = optparse.OptionParser()
    parser.add_option("--build_type", dest="build_type", default="RELEASE")
    parser.add_option("--compiler", dest="compiler")
    parser.add_option("--cxxflags", dest="cxxflags")
    parser.add_option("--output_dir", dest="output_dir", default="install")
    parser.add_option("--gpdb_name", dest="gpdb_name")
    (options, args) = parser.parse_args()
    status = install_system_deps()
    if status:
        return status
    for dependency in args:
        status = install_dependency(dependency)
        if status:
            return status
    status = install_gpdb(options.gpdb_name)
    if status:
        return status
    status = configure()
    if status:
        return status
    status = create_gpadmin_user()
    if status:
        return status
    status = icg()
    return status

if __name__ == "__main__":
    sys.exit(main())
