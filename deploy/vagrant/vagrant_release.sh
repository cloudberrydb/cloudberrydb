#!/bin/bash

#Example
#./vagrant_release.sh workspace_default_1658160749780_4821 ~/workspace/devel-devtoolset-10-cbdb-vagrant-virtualbox-centos7-2009-20220720.box devel-devtoolset-10-cbdb-vagrant-virtualbox-centos7-2009 20220720 ${key}

if [ $# -ne 5 ]; then
    echo "The input parameter is not correct!"
    exit 1
fi

box_name=${1}
image_path=${2}
image_name=${3}
image_version=${4}
apikey=${5}

#clean up
if [ -f ${image_path} ]
then
    rm ${image_path}
fi

#start to packaging the release image
packaging_cmd="vagrant package --base ${box_name} --output ${image_path}"
echo "[CBDB IMAGE RELEASE] start to packaging the release image with command: ${packaging_cmd}"
eval ${packaging_cmd}

#start to upload image
upload_cmd="curl -H 'X-JFrog-Art-Api:${apikey}' -T ${image_path} \"https://artifactory.hashdata.xyz/artifactory/vagrant-public/${image_name}-${image_version}.box;box_name=${image_name};box_provider=virtualbox;box_version=${image_version}\""
echo "[CBDB IMAGE RELEASE] start to upload image with curl command: $upload_cmd"
eval ${upload_cmd}

