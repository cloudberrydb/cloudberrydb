#!/usr/bin/env python
import subprocess
import os
import sys
import argparse
import shutil
from functools import wraps

BASE_WD = os.getcwd()
ORCA_SRC_DIR=os.path.join(BASE_WD, 'orca_src')
GPORCA_TAG_FILE='orca_github_release_stage/tag.txt'

def fail_on_error(func):
  def _check_status(request, *args, **kwargs):
    response = func(request, *args, **kwargs)
    if response:
      return sys.exit(1)
  return wraps(func)(_check_status)

def exec_command(cmd):
  print "Executing command: {0}".format(' '.join(cmd))
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
  while True:
    nextline = p.stdout.readline()
    if nextline == '' and p.poll() is not None:
      break
    sys.stdout.write(nextline)
    sys.stdout.flush()

  output = p.communicate()[0]
  exitCode = p.returncode
  print "Exit Code: {0}\n".format(p.returncode)
  return p.returncode == 1

def read_orca_version():
  # read orca version
  with open(GPORCA_TAG_FILE, 'r') as fh:
      return fh.read().strip();

@fail_on_error
def export_orca_src(orca_version):
  cmd = ["conan", "export", "orca/{0}@gpdb/stable".format(orca_version)]
  return exec_command(cmd)

@fail_on_error
def add_conan_remote(bintray_remote_name):
  # add conan remote
  cmd = ["conan", "remote",  "add", bintray_remote_name, "https://api.bintray.com/conan/greenplum-db/gpdb-oss"]
  return exec_command(cmd)

@fail_on_error
def set_bintray_user(bintray_user, bintray_user_key, bintray_remote):
  # set bintray account to be used to upload package
  cmd = ["conan", "user", "-p", bintray_user_key, "-r={0}".format(bintray_remote), bintray_user]
  return exec_command(cmd)

@fail_on_error
def upload_orca_src(orca_version):
  # upload orca source to bintray
  cmd = ["conan", "upload", "orca/{0}@gpdb/stable".format(orca_version), "--all", "-r=conan-gpdb"]
  return exec_command(cmd)

if __name__ == "__main__":
  parser = argparse.ArgumentParser(description='Main drived to build and install ORCA using conan')
  required_arguments = parser.add_argument_group('required arguments')
  required_arguments.add_argument('--bintrayUser', help='Bintray user name to upload packages', type=str)
  required_arguments.add_argument('--bintrayUserKey', help='Bintray user key', type=str)
  required_arguments.add_argument('--bintrayRemote', help='Name of conan remote refering to bintray', type=str)

  args = parser.parse_args()
  if args.bintrayRemote is None or args.bintrayUser is None or args.bintrayUserKey is None:
    print "Values for --bintrayRemote, --bintrayUser and --bintrayUserKey argument values are required, some are missing, exiting!"
    sys.exit(1)

  # setup
  orca_version = read_orca_version()
  print "Tag: {0}".format(orca_version)
  os.chdir(ORCA_SRC_DIR)
  shutil.copy("concourse/conanfile.py", "conanfile.py")

  # main conan driver
  export_orca_src(orca_version)
  add_conan_remote(args.bintrayRemote)
  set_bintray_user(args.bintrayUser, args.bintrayUserKey, args.bintrayRemote)
  upload_orca_src(orca_version)
