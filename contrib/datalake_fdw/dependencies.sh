#!/usr/bin/env bash

# parquet
HASHDATA_PARQUET_CONAN_VERSION=${HASHDATA_PARQUET_CONAN_VERSION:-parquet/0.0.1@hashdata/cbdb_"${OS}"_"${ARCH}"_"${BUILD_TYPE}"}

if [ ${GPHOME} ]; then
	BUILD_INSTALL_DIRECTORY="${GPHOME}"
else
	echo -e "\e[0Ksection_start:`date +%s`:install_conan_dependencies\r\e[0KError: set GPHOME enviroment variable first!!"
	exit 1
fi

function install_conan_dependencies() {

	cat <<EOF >/tmp/conanfile.txt
[requires]
${HASHDATA_PARQUET_CONAN_VERSION}

[imports]
bin, * -> ${BUILD_INSTALL_DIRECTORY}/bin
lib, * -> ${BUILD_INSTALL_DIRECTORY}/lib
include, * -> ${BUILD_INSTALL_DIRECTORY}/include
EOF

	sudo conan install /tmp/ --install-folder=${BUILD_INSTALL_DIRECTORY} --update
	chmod 0755 "${BUILD_INSTALL_DIRECTORY}"/bin/*
}

echo -e "\e[0Ksection_start:`date +%s`:install_conan_dependencies\r\e[0KInstall parquet..."
install_conan_dependencies
echo -e "\e[0Ksection_end:`date +%s`:install_conan_dependencies\r\e[0K"