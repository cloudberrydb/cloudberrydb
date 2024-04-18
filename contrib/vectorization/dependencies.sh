#!/usr/bin/env bash

# Get the dependences libraries for vectorization.
ARCH="$(uname -i)"
OS="$(. /etc/os-release && echo "${ID}")"
ARROW_BUILD_TYPE="release"
ARROW_BRANCH="numeric_new_type"
LAST_COMMIT=`git ls-remote git@code.hashdata.xyz:hashdata/arrow.git ${ARROW_BRANCH} | awk '{ print $1}'`

if [ $# -eq 1 ] && [ "${1,,}" = "debug" ]; then
  ARROW_BUILD_TYPE="debug"
fi

HASHDATA_ARROW_CONAN_VERSION=${HASHDATA_ARROW_CONAN_VERSION:-arrow/"${ARROW_BRANCH}"@hashdata/"${OS}"_"${ARCH}"_"${ARROW_BUILD_TYPE}"#"${LAST_COMMIT}"}
# orc, use arrow's dependency by default
HASHDATA_LIBORC_CONAN_VERSION=${HASHDATA_LIBORC_CONAN_VERSION:-orc/1.7.0@hashdata/"${OS}"_"${ARCH}"_"${ARROW_BUILD_TYPE}"}

BUILD_INSTALL_DIRECTORY=$(pwd)

function install_conan_dependencies() {
  if conan remote list | grep -q conan-center; then
    conan remote remove conan-center
  fi
  if conan remote list | grep -q conancenter; then
    conan remote remove conancenter
  fi
  if ! conan remote list | grep -q conan-public; then
    conan remote add conan-public https://artifactory.hashdata.xyz/artifactory/api/conan/conan-public
  fi

  # Auto detect default profile
  conan install xxx >/dev/null 2>&1 || true
  conan profile update settings.build_type=Release default
  conan profile update settings.compiler.libcxx=libstdc++11 default
  conan config set general.revisions_enabled=1

  cat <<EOF >$(pwd)/conanfile.txt
[requires]
${HASHDATA_ARROW_CONAN_VERSION}
${HASHDATA_LIBORC_CONAN_VERSION}

[imports]
bin, * -> $(pwd)/arrow/bin
lib, * -> $(pwd)/arrow/lib
include, * -> $(pwd)/arrow/include
EOF

  conan install $(pwd) --install-folder=${BUILD_INSTALL_DIRECTORY} --update
}

mkdir -p arrow/include arrow/lib
echo -e "\e[0Ksection_start:`date +%s`:install_conan_dependencies\r\e[0KInstall arrow and orc..."
install_conan_dependencies
echo -e "\e[0Ksection_end:`date +%s`:install_conan_dependencies\r\e[0K"
