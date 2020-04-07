#!/usr/bin/python

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

def install_dependencies(dependencies, output_dir):
    for dependency in dependencies:
        status = install_dependency(dependency, output_dir)
        if status:
            return status

def install_dependency(dependency_name, output_dir):
    return subprocess.call(
        ["tar -xzf " + dependency_name + "/*.tar.gz -C " + output_dir], shell=True)

def cmake_configure(src_dir, build_type, output_dir, cxx_compiler = None, cxxflags = None, thirty_two_bit = False):
    if os.path.exists("build"):
        os.removedirs("build")
    os.mkdir("build")
    cmake_args = ["cmake",
                  "-D", "CMAKE_BUILD_TYPE=" + build_type,
                  "-D", "CMAKE_INSTALL_PREFIX=../" + output_dir]
    if cxx_compiler:
        cmake_args.append("-D")
        cmake_args.append("CMAKE_CXX_COMPILER=" + cxx_compiler)
    if cxxflags:
        cmake_args.append("-D")
        cmake_args.append("CMAKE_CXX_FLAGS=" + cxxflags)
    if thirty_two_bit:
        cmake_args.append("-D")
        cmake_args.append("CMAKE_TOOLCHAIN_FILE=../" + src_dir + "/cmake/i386.toolchain.cmake")

    cmake_args.append("../" + src_dir)
    cmake_command = " ".join(cmake_args)
    if os.path.exists('/opt/gcc_env.sh'):
        cmake_command = "source /opt/gcc_env.sh && " + cmake_command
    print cmake_command
    return subprocess.call(cmake_command, cwd="build", shell=True)

def make():
    return subprocess.call(["make",
        "-j" + str(num_cpus()),
        "-l" + str(2 * num_cpus()),
        ],
        cwd="build",
        env=ccache_env()
        )


def ccache_env():
    env = os.environ.copy()
    env['CCACHE_DIR'] = os.getcwd() + '/.ccache'
    return env


def run_tests():
    return subprocess.call(["ctest",
                            "--output-on-failure",
                            "-j" + str(num_cpus()),
                            "--test-load", str(4 * num_cpus()),
                            ],
                            cwd="build")

def install():
    return subprocess.call(["make", "install"], cwd="build")

def main():
    parser = optparse.OptionParser()
    parser.add_option("--build_type", dest="build_type", default="RELEASE")
    parser.add_option("--compiler", dest="compiler")
    parser.add_option("--cxxflags", dest="cxxflags")
    parser.add_option("--output_dir", dest="output_dir", default="install")
    parser.add_option("--32", dest="thirty_two_bit", default=False)
    parser.add_option("--skiptests", dest="skiptests", action="store_true", default=False)

    (options, args) = parser.parse_args()
    # install deps for building
    status = install_dependencies(args, "/usr/local")
    if status:
        return status
    status = cmake_configure("orca_src",
                             options.build_type,
                             options.output_dir,
                             options.compiler,
                             options.cxxflags,
                             options.thirty_two_bit)
    if status:
        return status
    status = make()
    if status:
        return status
    if not options.skiptests:
        status = run_tests()
        if status:
            return status
    status = install()
    if status:
        return status
    status = install_dependencies(args, options.output_dir)
    if status:
        return status
    return 0

if __name__ == "__main__":
    sys.exit(main())
