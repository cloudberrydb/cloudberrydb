#!/usr/bin/python2

import optparse
import os
import subprocess
import sys

from builds.GpBuild import GpBuild

CODEGEN_MODE = 'codegen'
ORCA_CODEGEN_DEFAULT_MODE = "orca_codegen"
ORCA_MODE = 'orca'
PLANNER_MODE = 'planner'
INSTALL_DIR = "/usr/local/gpdb"


def make(num_cpus):
    return subprocess.call(["make",
                            "-j" + str(num_cpus),
                            "-l" + str(2 * num_cpus),
                            ],
                           cwd="gpdb_src")


def install():
    return subprocess.call("make install", cwd="gpdb_src", shell=True)


def copy_installed(output_dir):
    if os.path.normpath(INSTALL_DIR) != os.path.normpath(output_dir):
        status = subprocess.call("mkdir -p " + output_dir, shell=True)
        if status:
            return status
        return subprocess.call("cp -r %s/*  %s" % (INSTALL_DIR, output_dir), shell=True)
    return


def unittest():
    return subprocess.call("make -s unittest-check", cwd="gpdb_src/src/backend", shell=True)


def print_compiler_version():
    status = subprocess.call(["g++", "--version"])
    if status:
        return status
    return subprocess.call(["gcc", "--version"])


def main():
    parser = optparse.OptionParser()
    parser.add_option("--build_type", dest="build_type", default="RELEASE")
    parser.add_option("--mode", choices=[ORCA_MODE, CODEGEN_MODE, ORCA_CODEGEN_DEFAULT_MODE, PLANNER_MODE], default=ORCA_CODEGEN_DEFAULT_MODE)
    parser.add_option("--compiler", dest="compiler")
    parser.add_option("--cxxflags", dest="cxxflags")
    parser.add_option("--output_dir", dest="output_dir", default=INSTALL_DIR)
    (options, args) = parser.parse_args()
    ci_common = GpBuild(ORCA_CODEGEN_DEFAULT_MODE)
    if options.mode == ORCA_MODE:
        ci_common = GpBuild(options.mode)
    elif options.mode == PLANNER_MODE:
        ci_common = GpBuild(options.mode)

    for dependency in args:
        status = ci_common.install_dependency(dependency)
        if status:
            return status
    status = print_compiler_version()
    if status:
        return status
    status = ci_common.configure()
    if status:
        return status
    status = make(ci_common.num_cpus())
    if status:
        return status
    status = unittest()
    if status:
        return status
    status = install()
    if status:
        return status
    status = copy_installed(options.output_dir)
    if status:
        return status
    return 0


if __name__ == "__main__":
    sys.exit(main())
