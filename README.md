<pre>
======================================================================
               __________        ____  ____  _________ 
              / ____/ __ \      / __ \/ __ \/ ____/   |
             / / __/ /_/ /_____/ / / / /_/ / /   / /| |
            / /_/ / ____/_____/ /_/ / _, _/ /___/ ___ |
            \____/_/          \____/_/ |_|\____/_/  |_|
                  The Greenplum Query Optimizer
              Copyright (c) 2015, Pivotal Software, Inc.
            Licensed under the Apache License, Version 2.0
======================================================================
</pre>

Welcome to GP-ORCA, the Greenplum Next Generation Query Optimizer!

GP-ORCA supports various build types: debug, release with debug info, release.
On x86 systems, GP-ORCA can also be built as a 32-bit or 64-bit library. You'll
need CMake 3.0 or higher to build GP-ORCA. Get it from cmake.org, or your
operating system's package manager.

## Quick Start: Build GP-ORCA and install under /usr/local

```
% mkdir build
% cd build
% cmake ../
% make
% sudo make install
```

Or read on for more detailed instructions below...

## Pre-Requisites

GP-ORCA uses the following libraries:
1) GPOS - Greenplum's OS Abstraction Layer
2) GP-Xerces - Greenplum's patched version of Xerces-C 3.1.X

### Installing GPOS

[GPOS is available here](https://github.com/greenplum-db/gpos) The GPOS README
gives instructions for building and installing GPOS. Note that the build type
(e.g. DEBUG vs. RELEASE) for GPOS and GP-ORCA should match (mixing and matching
can lead to errors).

If GPOS was installed to the default location, the cmake build system for
GP-ORCA should find it automatically. Otherwise, cmake can be pointed to your
GPOS installation with the `GPOS_INCLUDE_DIR` and `GPOS_LIBRARY` options like
so:

```
cmake -D GPOS_INCLUDE_DIR=/opt/gpos/include -D GPOS_LIBRARY=/opt/gpos/lib/libgpos.so ..
```

Note that on Mac OS X, the library name will end with `.dylib` instead of `.so`.

### Installing GP-Xerces

To install GP-Xerces, obtain a copy of the
[Xerces-C source code](https://xerces.apache.org/xerces-c/download.cgi)
(versions 3.1.1 and 3.1.2 are tested and known to work) and apply the the GPDB
patchset (located at `patches/xerces-c-gpdb.patch` in the GP-Orca source tree)
before building. Presuming that you downloaded Xerces-C 3.1.2, a recipe for
building GP-Xerces would be something like the following:

```
% tar -xzf xerces-c-3.1.2.tar.gz
% cd xerces-c-3.1.2
% patch -p1 < /path/to/orca/patches/xerces-c-gpdb.patch
% mkdir build
% cd build
% ../configure --prefix=/opt/gp_xerces
% make
% sudo make install
```

It is recommended to use the `--prefix` option to the Xerces-C configure script
to install GP-Xerces in a location other than the default under `/usr/local/`,
because you may have other software that depends on Xerces-C, and the changes
introduced in the GP-Xerces patch make it incompatible with the upstream
version. Installing in a non-default prefix allows you to have GP-Xerces
installed side-by-side with unpatched Xerces without incompatibilities.

You can point cmake at your patched GP-Xerces installation using the
`XERCES_INCLUDE_DIR` and `XERCES_LIBRARY` options like so:

```
cmake -D XERCES_INCLUDE_DIR=/opt/gp_xerces/include -D XERCES_LIBRARY=/opt/gp_xerces/lib/libxerces-c.so ..
```

Again, on Mac OS X, the library name will end with `.dylib` instead of `.so`.

**Advanced - Cross-compiling 32-bit or 64-bit libraries** Unless you intend to
cross-compile a 32 or 64-bit version of GP-Orca, you can ignore these
instructions. If you need to explicitly compile for the 32 or 64-bit version of
your architecture, you need to set the `CFLAGS` and `CXXFLAGS` environment
variables for the configure script like so (use `-m32` for 32-bit, `-m64` for
64-bit):

```
CFLAGS="-m32" CXXFLAGS="-m32" ../configure --prefix=/opt/gp_xerces_32
```

## Preperation for build


Go into gp-orca and create a build folder
```
% mkdir build
% cd build
```

### How to generate make files with default options
Please ensure that build type of GPOS matches the version of Optimizer libraries
you are trying to build. Mixing and matching a DEBUG GPOS with a RELEASE Orca or
vice-versa may cause problems.

* debug build

```
% cmake -D CMAKE_BUILD_TYPE=DEBUG ../
```

* release build with debug info

```
% cmake -D CMAKE_BUILD_TYPE=RelWithDebInfo ../
```

* release build

```
% cmake -D CMAKE_BUILD_TYPE=RELEASE ../
```

## Explicitly Specifying GPOS and GP-Xerces For Build

As noted in the prerequisites section above, you may specify the
`GPOS_INCLUDE_DIR` and `GPOS_LIBRARY` options to tell cmake where to find
GPOS (or similarly with `XERCES_INCLUDE_DIR` and `XERCES_LIBRARY` for
GP-Xerces). These options are useful if GPOS/GP-Xerces is installed in a
nonstandard place, or if multiple versions are installed in different locations
(for instance DEBUG vs. RELEASE, or 32 vs. 64-bit builds).

For example:
```
cmake -D XERCES_INCLUDE_DIR=/opt/gp_xerces/include -D XERCES_LIBRARY=/opt/gp_xerces/lib/libxerces-c.so ../
```

## Advanced: Cross-Compiling 32-bit or 64-bit libraries

For the most part you should not need to explicitly compile a 32-bit or 64-bit
version of the optimizer libraries. By default, a "native" version for your host 
platform will be compiled. However, if you are on x86 and want to, for example, 
build a 32-bit version of Optimizer libraries on a 64-bit machine, you can do 
so as described below. Note that you will need a "multilib" C++ compiler that 
supports the -m32/-m64 switches, and you may also need to install 32-bit ("i386") 
versions of the C and C++ standard libraries for your OS. Finally, you will need
to build 32-bit or 64-bit versions of GPOS and GP-Xerces as appropriate.

Toolchain files for building 32 or 64-bit x86 libraries are located in the cmake
directory. Here is an example of building for 32-bit x86:

```
cmake -D CMAKE_TOOLCHAIN_FILE=../cmake/i386.toolchain.cmake ../
```

And for 64-bit x86:

```
cmake -D CMAKE_TOOLCHAIN_FILE=../cmake/x86_64.toolchain.cmake ../
```

## How to build

* build

```
% make
```

* for faster build use the -j option of make. For instance, the following command runs make on 7 job slots

```
% make -j7
```

* show all commands being run as part of make

```
% make VERBOSE=1
```

## How to test

To run all GP-ORCA tests, simply use the `ctest` command from the build directory
after `make` finishes.

```
% ctest
```

Much like `make`, `ctest` has a -j option that allows running multiple tests in
parallel to save time. Using it is recommended for faster testing.

```
% ctest -j7
```

By default, `ctest` does not print the output of failed tests. To print the
output of failed tests, use the `--output-on-failure` flag like so (this is
useful for debugging failed tests):

```
% ctest -j7 --output-on-failure
```

To run a specific individual test, use the `gporca_test` executable directly.

```
./server/gporca_test -U CAggTest
```

Note that some tests use assertions that are only enabled for DEBUG builds, so
DEBUG-mode tests tend to be more rigorous.

### Advanced: Extended Tests

Debug builds of GP-ORCA include a couple of "extended" tests for features like
fault-simulation and time-slicing that work by running the entire test suite
in combination with the feature being tested. These tests can take a long time
to run and are not enabled by default. To turn extended tests on, add the cmake
arguments `-D ENABLE_EXTENDED_TESTS=1`.

## How to install

GP-ORCA has three libraries:

1. libnaucrates --- has all DXL related classes, and statistics related classes
2. libgpopt     --- has all the code related to the optimization engine, meta-data accessor, logical / physical operators, 
                    transformation rules, and translators (DXL to expression and vice versa). 
3. libgpdbcost  --- cost model for GPDB. 

By default, GP-ORCA will be installed under /usr/local. You can change this by
setting CMAKE_INSTALL_PREFIX when running cmake, for example:
```
% cmake -D CMAKE_INSTALL_PREFIX=/home/user/gp-orca ../
```

By default, the header files are located in:
```
/usr/local/include/naucrates
/usr/local/include/gpdbcost
/usr/local/include/gpopt
```
the library is located at:

```
/usr/local/lib/libnaucrates.so*
/usr/local/lib/libgpdbcost.so*
/usr/local/lib/libgpopt.so*
```

* build and install
```
% make install
```
* build and install with verbose output
```
% make VERBOSE=1 install
```

## Clean up stuff 

* remove the cmake files generated under build

* Remove gp-orca header files and library, (assuming the default install prefix /usr/local)

```
% rm -rf /usr/local/include/naucrates
% rm -rf /usr/local/include/gpdbcost
% rm -rf /usr/local/include/gpopt
% rm -rf /usr/local/lib/libnaucrates.so*
% rm -rf /usr/local/lib/libgpdbcost.so*
% rm -rf /usr/local/lib/libgpopt.so*
```