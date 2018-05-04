#!/usr/bin/env python
import subprocess
import os
import sys

outputDirectory = os.environ['outputDirectory']
build_type = os.environ['build_type']
if build_type == 'DEBUG':
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
build_and_test_orca_cmd = "orca_main_src/concourse/build_and_test.py --build_type={0} --output_dir={1}/install bin_xerces_centos5".format(build_type, outputDirectory)
exec_command(build_and_test_orca_cmd)
package_tarball_cmd = "env dst_tarball=package_tarball_{0}/bin_orca_centos5_{0}.tar.gz src_root={1}/install orca_main_src/concourse/package_tarball.bash".format(path_identifier, outputDirectory)
exec_command(package_tarball_cmd)
