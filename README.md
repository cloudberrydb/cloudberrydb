# ======================================================================
#                          __ _ _ __   ___  ___
#                         / _` | '_ \ / _ \/ __|
#                        | (_| | |_) | (_) \__ \
#                         \__, | .__/ \___/|___/
#                         |___/|_|
#
#                 The Greenplum OS abstraction layer!
# ----------------------------------------------------------------------
# Copyright (c) 2015, Pivotal Software, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ======================================================================

GPOS supports various build types: debug, release with debug info, release.
On x86 systems, GPOS can also be built as a 32-bit or 64-bit library. You'll
need CMake 3.0 or higher to build GPOS. Get it from cmake.org, or your
operating system's package manager.

====================================================
Quick Start: Build GPOS and install under /usr/local
====================================================

	% cd gpos
	% mkdir build
	% cd build
	% cmake ../gpos
	% make
	% sudo make install


Or read on for more detailed instructions below...

=====================
Preperation for build
=====================

Go into gpos and create a build folder

	% cd gpos
	% mkdir build
	% cd build

=================================================
How to generate make files with default options
=================================================

--- debug build
	% cmake -D CMAKE_BUILD_TYPE=DEBUG ../gpos
  or 
--- release build with debug info
	% cmake -D CMAKE_BUILD_TYPE=RelWithDebInfo ../gpos
  or
--- release build
	% cmake -D CMAKE_BUILD_TYPE=RELEASE ../gpos

===============================================================================================
ADVANCED: How to generate make files using toolchain to generate 32 or 64 bit version makefiles 
===============================================================================================

For the most part you should not need to explicitly compile a 32-bit or 64-bit
version of GPOS. By default, a "native" version for your host platform will be
compiled. However, if you are on x86 and want to, for example, build a 32-bit
version of GPOS on a 64-bit machine, you can do so as described below. Note
that you will need a "multilib" C++ compiler that supports the -m32/-m64
switches, and you may also need to install 32-bit ("i386") versions of the C
and C++ standard libraries for your OS.

--- debug version with verbose install path

--- 32-bit x86

	% cmake -D VERBOSE_INSTALL_PATH=1 -D CMAKE_BUILD_TYPE=DEBUG -D CMAKE_TOOLCHAIN_FILE=../gpos/i386.toolchain.cmake ../gpos

--- 64-bit x86

	% cmake -D VERBOSE_INSTALL_PATH=1 -D CMAKE_BUILD_TYPE=DEBUG -D CMAKE_TOOLCHAIN_FILE=../gpos/x86_64.toolchain.cmake ../gpos

=============
How to build
=============

--- build
	% make

--- for faster build use the -j option of make
--- for instance, the following command runs make on 7 job slots
	% make -j7

--- show all commands being run as part of make
	% make VERBOSE=1

=============
How to test
=============

--- running all tests Option 1:
	% make test

--- running all tests
./gpos_test -u

--- running a particular test
./gpos_test -U CAutoTaskProxyTest

===============
How to install
===============

By default, GPOS will be installed under /usr/local. You can change this by
setting CMAKE_INSTALL_PREFIX when running cmake like so:

	% cmake -D CMAKE_INSTALL_PREFIX=/home/zaphod/gpos ../gpos

--- If VERBOSE_INSTALL_PATH was not set during cmake then 
--- the header files are located in /usr/local/include/gpos
--- the library is located at /usr/local/lib/libgpos.so
--- (or similar paths under a different prefix)

--- If VERBOSE_INSTALL_PATH was turned on during cmake then 
--- the header files and libraries are located under a subdirectory of
--- /usr/local/libgpos

--- build and install
	% make install

--- build and install with verbose output
	% make VERBOSE=1 install

===============
Clean up stuff 
===============

-- remove the cmake files generated under build
	% rm -rf *

--- If VERBOSE_INSTALL_PATH was not use during cmake then remove gpos like so
--- (assuming the default install prefix /usr/local)
	% rm -rf /usr/local/include/gpos
	% rm -rf /usr/local/lib/libgpos.so*

--- If VERBOSE_INSTALL_PATH was used during cmake then clean up the appropriate
--- libraries and header files under /usr/local/libgpos (assuming the default
--- install prefix /usr/local)
