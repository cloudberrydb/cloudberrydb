[![Build Status](https://travis-ci.org/xinzweb/gpos.svg?branch=master)](https://travis-ci.org/xinzweb/gpos)

<pre>
======================================================================
					   __________  ____  _____
					  / ____/ __ \/ __ \/ ___/
					 / / __/ /_/ / / / /\__ \ 
					/ /_/ / ____/ /_/ /___/ / 
					\____/_/    \____//____/  
					                          
                The Greenplum OS abstraction layer!
----------------------------------------------------------------------
Copyright (c) 2015, Pivotal Software, Inc.

Licensed under the Apache License, Version 2.0
======================================================================
</pre>

Welcome to GPOS, the Greenplum OS abstraction layer!

GPOS supports various build types: debug, release with debug info, release.
On x86 systems, GPOS can also be built as a 32-bit or 64-bit library. You'll
need CMake 3.0 or higher to build GPOS. Get it from cmake.org, or your
operating system's package manager.


## Quick Start: Build GPOS and install under /usr/local

```
mkdir build
cd build
cmake ../
make
sudo make install
```

Or read on for more detailed instructions below...


## Preperation for build


Go into gpos and create a build folder
```
mkdir build
cd build
```

## How to generate make files with default options

* debug build
```
cmake -D CMAKE_BUILD_TYPE=DEBUG ../
```
  or 
* release build with debug info
```
cmake -D CMAKE_BUILD_TYPE=RelWithDebInfo ../
```
  or
* release build
```
cmake -D CMAKE_BUILD_TYPE=RELEASE ../
```

## Advanced: How to generate make files using toolchain to generate 32 or 64 bit version makefiles 

For the most part you should not need to explicitly compile a 32-bit or 64-bit
version of GPOS. By default, a "native" version for your host platform will be
compiled. However, if you are on x86 and want to, for example, build a 32-bit
version of GPOS on a 64-bit machine, you can do so as described below. Note
that you will need a "multilib" C++ compiler that supports the -m32/-m64
switches, and you may also need to install 32-bit ("i386") versions of the C
and C++ standard libraries for your OS.

Debug version with verbose install path:

* 32-bit x86
```
cmake -D VERBOSE_INSTALL_PATH=1 -D CMAKE_BUILD_TYPE=DEBUG -D CMAKE_TOOLCHAIN_FILE=../i386.toolchain.cmake ../
```
* 64-bit x86
```
cmake -D VERBOSE_INSTALL_PATH=1 -D CMAKE_BUILD_TYPE=DEBUG -D CMAKE_TOOLCHAIN_FILE=../x86_64.toolchain.cmake ../
```

## How to build

* build
```
make
```
* for faster build use the -j option of make. For instance, the following command runs make on 7 job slots
```
make -j7
```
* show all commands being run as part of make
```
make VERBOSE=1
```

## How to test

To run all GPOS tests, simply use the `ctest` command from the build directory
after `make` finishes.
```
ctest
```

Much like `make`, `ctest` has a -j option that allows running multiple tests in
parallel to save time. Using it is recommended for faster testing.
```
ctest -j7
```

By default, `ctest` does not print the output of failed tests. To print the
output of failed tests, use the `--output-on-failure` flag like so (this is
useful for debugging failed tests):
```
ctest -j7 --output-on-failure
```

To run a specific individual test, use the `gpos_test` executable directly.
```
./server/gpos_test -U CAutoTaskProxyTest
```

Note that some tests use assertions that are only enabled for DEBUG builds, so
DEBUG-mode tests tend to be more rigorous.

### Advanced: Extended Tests

Debug builds of GPOS include a couple of "extended" tests for features like
fault-simulation and time-slicing that work by running the entire test suite
in combination with the feature being tested. These tests can take a long time
to run and are not enabled by default. To turn extended tests on, add the cmake
arguments `-D ENABLE_EXTENDED_TESTS=1`.

## How to install

By default, GPOS will be installed under /usr/local. You can change this by
setting CMAKE_INSTALL_PREFIX when running cmake, for example:
```
cmake -D CMAKE_INSTALL_PREFIX=/home/user/gpos ../
```
If VERBOSE_INSTALL_PATH was not set during cmake then 
the header files are located in /usr/local/include/gpos
the library is located at /usr/local/lib/libgpos.so
(or similar paths under a different prefix)

If VERBOSE_INSTALL_PATH was turned on during cmake then 
the header files and libraries are located under a subdirectory of
/usr/local/libgpos

* build and install
```
make install
```
* build and install with verbose output
```
make VERBOSE=1 install
```
## Clean up stuff 

* remove the cmake files generated under build

* If VERBOSE_INSTALL_PATH was not use during cmake then remove gpos header files and library, (assuming the default install prefix /usr/local)
```
rm -rf /usr/local/include/gpos
rm -rf /usr/local/lib/libgpos.so*
```
If VERBOSE_INSTALL_PATH was used during cmake then clean up the appropriate
libraries and header files under /usr/local/libgpos (assuming the default
install prefix /usr/local)
