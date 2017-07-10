#!/usr/bin/env python
import subprocess
import sys
from packaging import version


if __name__ == "__main__":
  cmd = subprocess.Popen(['env', 'GIT_DIR=orca_src/.git', 'git', 'describe', '--abbrev=0', '--tags'], stdout=subprocess.PIPE)
  existing_tag = cmd.stdout.read()
  print ("Latest Tag of ORCA: {0}".format(existing_tag.strip()))

  tag_to_be_published = ''
  with open('orca_github_release_stage/tag.txt', 'r') as fh:
    tag_to_be_published = fh.read().strip()

  if tag_to_be_published == '':
     sys.exit('Unable to read the tag from tag.txt')

  print ("Tag from this commit: {0}".format(tag_to_be_published))

  if (version.parse(existing_tag) >= version.parse(tag_to_be_published)):
    print ("Tag {0} already present on ORCA repository".format(tag_to_be_published))
    print ("Please BUMP the ORCA version")
    sys.exit(1)
