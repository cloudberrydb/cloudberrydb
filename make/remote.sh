#!/bin/sh 

TAR=gtar
MAKE=make

# untar into the remote target directory
mkdir $REMDIR
cd $REMDIR
$TAR xfz $REMTMP/$ARCHIVE

# touch all files to avoid make's complaints about clock skew
find . -type f | xargs touch

# clean up just in case
make purge

# override auto make flags to monitor progress
make auto AUTO_MAKE_FLAGS=

