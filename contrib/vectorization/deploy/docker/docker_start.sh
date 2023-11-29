#!/bin/bash
# set user env
CBDB_DIR="$1"
CONTAINER_DIR="$2"

# set image
IMAGE_NAME="cbdb-arrow"
CONTAINER_NAME="cbdb-arrow"

# build container
echo "Building Docker image..."
docker build -t $IMAGE_NAME .

echo "--- ${CBDB_DIR}"
# run container
echo "Starting Docker container..."
docker run -e CONTAINER_DIR=${CONTAINER_DIR} -idt -v $CBDB_DIR:$CONTAINER_DIR --cap-add sys_ptrace --ulimit core=-1 --privileged --name $CONTAINER_NAME $IMAGE_NAME

# exec container
echo "Entering Docker container..."
docker exec -u gpadmin -it $CONTAINER_NAME /bin/zsh
