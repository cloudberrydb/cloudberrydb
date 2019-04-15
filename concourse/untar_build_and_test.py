#!/usr/bin/env python
import subprocess
import os
import sys

BUILD_TYPE=os.environ['BUILD_TYPE']
OUTPUT_DIR=os.environ['OUTPUT_DIR']
SKIP_TESTS=os.environ['SKIP_TESTS']
if 'DEBUG' in BUILD_TYPE:
    path_identifier = 'debug'
else:
    path_identifier = 'release'

def exec_command(cmd):
  print "Executing command: {0}".format(cmd)
  retcode = subprocess.call(cmd, shell=True)
  if retcode != 0:
    sys.exit(retcode)

untar_orca_cmd = "mkdir -p orca_src && tar -xf orca_tarball/orca_src.tar.gz -C orca_src --strip 1"
exec_command(untar_orca_cmd)
build_and_test_orca_cmd = "orca_main_src/concourse/build_and_test.py {0} {1} {2} bin_xerces_centos5".format(BUILD_TYPE, OUTPUT_DIR, SKIP_TESTS)
exec_command(build_and_test_orca_cmd)

SRC_DIR=OUTPUT_DIR.split('=')[1]
package_tarball_cmd = "env dst_tarball=package_tarball/bin_orca_centos5_{0}.tar.gz src_root={1} orca_main_src/concourse/package_tarball.bash".format(path_identifier, SRC_DIR)
exec_command(package_tarball_cmd)
