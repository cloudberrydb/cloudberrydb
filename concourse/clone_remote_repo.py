#!/usr/bin/env python
import optparse
import subprocess
import sys

def read_commit_info(filename):
  with open(filename, 'r') as fp:
    data = fp.readline().strip('\n')

  return data.split(',')[0], data.split(',')[1]

def create_tarball(directory):
  tar_cmd = 'mkdir -p package_tarball && tar -cvzf package_tarball/{0}.tar.gz {0}'.format(directory)
  exec_command(tar_cmd)

def exec_command(cmd):
  print "Executing command: {0}".format(cmd)
  p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
  output = p.communicate()[0]
  if p.returncode != 0:
    sys.exit(p.returncode)

def remote_checkout(branch_or_commit, remote_url, dirname):
  cmd = "git clone {0} {1}".format(remote_url, dirname)
  exec_command(cmd)
  cmd = "pushd {0} && git checkout {1} && popd".format(dirname, branch_or_commit)
  exec_command(cmd)


if __name__ == "__main__":
  parser = optparse.OptionParser()
  parser.add_option("--orcaRemoteInfo", dest="orcaRemoteInfo", default="gporca-commits-to-test/orca_remote_info.txt")
  parser.add_option("--gpdbRemoteInfo", dest="gpdbRemoteInfo", default="gporca-commits-to-test/gpdb_remote_info.txt")

  (options, args) = parser.parse_args()

  # read commit / branch info and repo url
  orca_branch, orca_remote = read_commit_info(options.orcaRemoteInfo)
  gpdb_branch, gpdb_remote = read_commit_info(options.gpdbRemoteInfo)

  print "orca commit: {0}, orca remote: {1}".format(orca_branch, orca_remote)
  print "gpdb commit: {0}, gpdb remote: {1}".format(gpdb_branch, gpdb_remote)

  # checkout the branch from the given repo
  remote_checkout(orca_branch, orca_remote, 'orca_src')
  remote_checkout(gpdb_branch, gpdb_remote, 'gpdb_src')

  # create tarballs for orca and gpdb
  create_tarball('orca_src')
  create_tarball('gpdb_src')
