### Cloudberry Automation Build and Deployment

#### Download the Cloudberry project source code into your working directory
git clone git@github.com:cloudberrydb/cloudberrydb.git

#### Enter the directory under workspace/cbdb, developer can deploy the automation build & test environment with vagrant virtual machine or docker container, vagrant and docker tool is required to be installed in your local environment before running the script.

##### 1.Deploy the automation build & test environment with vagrant virtual machine

##### Start vagrant virtual machine by following script
cd ~/workspace/cbdb/deploy/vagrant
vagrant up

##### After virtual machine vbox being launched, compiled and deployed automatically by "vagrant up" script, then log into vagrant virtual machine via ssh
vagrant ssh
psql postgres

##### start regression test
cd ~/workspace/cbdb_dev
make installcheck-good


##### 2.Deploy the automation build & test environment with docker container

##### docker image repository configuration
Configure the docker image repository URL by adding the following JSON format configuration in "Preferences->Docker Engine" tab in Docker tool.
"insecure-registries": [
    "docker.artifactory.hashdata.xyz"
]

##### Start the docker container by following script
cd ~/workspace/cbdb/deploy/docker
bash docker_start.sh

##### After the docker container being launched, compiled and deployed automatically by docker_start.sh script, then log into the docker container. The ContainerId could be retrived by using "docker ps -a" command
docker exec -it [ContainerId] /bin/bash
psql postgres

##### start regression test
cd ~/workspace/cbdb_dev
make installcheck-good

<br>


### Script Introduction

The build & test environment of cbdb project relies on many systemconfigurations and third-party dependencies, therefore the manual deployment process is quite cumbersome and error-prone. For this reason, the automation build & test environment of cbdb is introduced by profitting the virtualization techologies such as vagrant or docker, which can be used to simplify the user's deployment efforts and time-costs to deploy the cbdb build & test environment. As a result the cbdb developers could be more concentrated on their business-level code development work. The cbdb project supports two automation deployment methods: the first one is to deploy vbox virtual machine with vagrant, and another one is to deploy container image with docker. Developers can choose the way of automation deployment depends on their own requirements. The entire automation build & test process is running on the virtualization environment managed by vagrant or docker. The cbdb code repository of the host machine is mapped into specified path in the virtualization environment through the sharing file mechanism for cbdb automation build & test.

#### The whole script is subdivided into three main parts:

##### 1.The automation deployment script running in the virtualization environment.
##### Through the mechanism provided by vagrant vagrantfile provision or docker dockerfile ENTRYPOINTS
  *  Provides subcommands that operates the build & test environment
  *  Provides the necessary configuration variables to the internal script of the virtual machine and trigger specific operations
##### Passing the following compilation options through /code/depoly/cbdb_ deploy.sh script
  *  Compilation options
  *  Deployment options
  *  Test options
##### Passing the environment variable parameters required for automation build & test through /code/depoly/cbdb_env.sh script.

#### 2.Vagrant startup script
#####   Vagrantfile
  *  Downloads the Vagrant virtual machine box and local virtual box creation.
  *  The host code repository path is mounted on virtual machine with read-only permission, for example: source code path to /home/gpadmin/workspace/cbdb directory
  *  Copy the source file directory to /home/gpadmin/workspace/cbdb_dev, which is the actual working directory for automation build & test.
  *  Ensure that the login user is gpadmin
#####   vagrant_release.sh
  *  Script which provides the packaging of vagrant box images.

#### 3.Docker startup script
#####   Dockerfile
  *  Docker image file packaging
  *  Docker container service startup configuration, such as SSH service
#####   docker_deploy.h
  *  Generates SSH public keys and automatic SSH login configuration
  *  Start docker automation deployment for build & test
#####   docker_start.sh
  *  Downloads docker image version remotely
  *  The host code repository path is mounted on virtual machine with read-only permission, for example: source code path to /home/gpadmin/workspace/cbdb directory
  *  Copy the source file directory to /home/gpadmin/workspace/cbdb_dev, which is the actual working directory for automation build & test.
  *  Ensure that the login user is gpadmin

<br>
