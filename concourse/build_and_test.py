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

def cmake_configure(src_dir, build_type, cxx_compiler = None, cxxflags = None):
    os.mkdir("build")
    cmake_args = ["cmake",
                  "-D", "CMAKE_BUILD_TYPE=" + build_type,
                  "-D", "CMAKE_INSTALL_PREFIX=../install"]
    if cxx_compiler:
        cmake_args.append("-D")
        cmake_args.append("CMAKE_CXX_COMPILER=" + cxx_compiler)
    if cxxflags:
        cmake_args.append("-D")
        cmake_args.append("CMAKE_CXX_FLAGS=" + cxxflags)
    cmake_args.append("../" + src_dir)
    subprocess.check_call(cmake_args, cwd="build")

def make():
    subprocess.check_call(["make", "-j" + str(num_cpus())], cwd="build")

def run_tests():
    subprocess.check_call(["ctest",
                           "--output-on-failure",
                           "-j" + str(num_cpus())], cwd="build")

def install():
    subprocess.check_call(["make", "install"], cwd="build")

def main():
    parser = optparse.OptionParser()
    parser.add_option("--build-type", dest="build_type", default="RELEASE")
    parser.add_option("--compiler", dest="compiler")
    parser.add_option("--cxxflags", dest="cxxflags")
    (options, args) = parser.parse_args()
    if len(args) > 0:
        print "Unknown arguments"
        return 1
    cmake_configure("gpos_src",
                    options.build_type,
                    options.compiler,
                    options.cxxflags)
    make()
    run_tests()
    install()
    return 0

if __name__ == "__main__":
    sys.exit(main())
