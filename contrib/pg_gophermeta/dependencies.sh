#!/usr/bin/env bash

# gopher
#HASHDATA_GOPHER_CONAN_VERSION=${HASHDATA_GOPHER_CONAN_VERSION:-gopher/master@hashdata/testing}

HASHDATA_GOPHER_CONAN_VERSION=${HASHDATA_GOPHER_CONAN_VERSION:-gopher/master@hashdata/testing}

# libhdfs3, use gopher's dependency by default
HASHDATA_LIBHDFS3_CONAN_VERSION=${HASHDATA_LIBHDFS3_CONAN_VERSION:-}

# liboss2, use gopher's dependency by default
HASHDATA_LIBOSS2_CONAN_VERSION=${HASHDATA_LIBOSS2_CONAN_VERSION:-}

# plasma, use gopher's dependency by default
HASHDATA_PLASMA_CONAN_VERSION=${HASHDATA_PLASMA_CONAN_VERSION:-}

if [ ${GPHOME} ]; then
	BUILD_INSTALL_DIRECTORY="${GPHOME}"
else
	echo -e "\e[0Ksection_start:`date +%s`:install_conan_dependencies\r\e[0KError: set GPHOME enviroment variable first!!"
	exit 1
fi


function install_conan_dependencies() {

	cat <<EOF >/tmp/conanfile.txt
[requires]
${HASHDATA_GOPHER_CONAN_VERSION}
${HASHDATA_LIBHDFS3_CONAN_VERSION}
${HASHDATA_LIBOSS2_CONAN_VERSION}
${HASHDATA_PLASMA_CONAN_VERSION}

[imports]
bin, * -> ${BUILD_INSTALL_DIRECTORY}/bin
lib, * -> ${BUILD_INSTALL_DIRECTORY}/lib
include, * -> ${BUILD_INSTALL_DIRECTORY}/include
EOF

	conan install /tmp/ --install-folder=${BUILD_INSTALL_DIRECTORY} --update
	chmod 0755 "${BUILD_INSTALL_DIRECTORY}"
}

echo -e "\e[0Ksection_start:`date +%s`:install_conan_dependencies\r\e[0KInstall gopher and liboss2 libhdfs3..."
install_conan_dependencies
echo -e "\e[0Ksection_end:`date +%s`:install_conan_dependencies\r\e[0K"
