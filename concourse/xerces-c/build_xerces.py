#!/usr/bin/python2

import optparse
import os
import os.path
import subprocess
import sys
import tarfile
import urllib2

XERCES_SOURCE_URL = "http://www.trieuvan.com/apache/xerces/c/3/sources/xerces-c-3.1.2.tar.gz"
XERCES_SOURCE_DIR = "xerces-c-3.1.2"

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

def get_xerces_source():
    remote_src = urllib2.urlopen(XERCES_SOURCE_URL)
    local_src = open("xerces_src.tar.gz", "w")
    local_src.write(remote_src.read())
    local_src.close()
    tarball = tarfile.open("xerces_src.tar.gz", "r:gz")
    for item in tarball:
        tarball.extract(item, ".")
    tarball.close()
    patchfile = open("xerces_patch/patches/xerces-c-gpdb.patch", "r")
    return subprocess.call(
        ["patch", "-p1"],
        stdin = patchfile,
        cwd = XERCES_SOURCE_DIR)

def configure(cxx_compiler, cxxflags):
    os.mkdir("build")
    environment = os.environ.copy()
    if cxx_compiler:
        environment["CXX"] = cxx_compiler
    if cxxflags:
        environment["CXXFLAGS"] = cxxflags
    return subprocess.call(
        [os.path.abspath(XERCES_SOURCE_DIR + "/configure"), "--prefix=" + os.path.abspath("install")],
        env = environment,
        cwd = "build")

def make():
    return subprocess.call(["make", "-j" + str(num_cpus())], cwd="build")

def install():
    return subprocess.call(["make", "install"], cwd="build")

def main():
    parser = optparse.OptionParser()
    parser.add_option("--compiler", dest="compiler")
    parser.add_option("--cxxflags", dest="cxxflags")
    (options, args) = parser.parse_args()
    if len(args) > 0:
        print "Unknown arguments"
        return 1
    status = get_xerces_source()
    if status:
        return status
    status = configure(options.compiler, options.cxxflags)
    if status:
        return status
    status = make()
    if status:
        return status
    status = install()
    if status:
        return status
    return 0

if __name__ == "__main__":
    sys.exit(main())
