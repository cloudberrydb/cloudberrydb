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
<br>


### Cloudberry自动编译部署流程

#### 在工作目录下载cloudberry的代码

git clone git@code.hashdata.xyz:cloudberry/cbdb.git

#### 进入workspace/cbdb目录下的deploy目录, 可以通过vagrant虚拟机或者docker容器方式部署编译测试环境。

##### 1.通过vagrant虚拟机方式部署编译测试环境

##### 启动vagrant虚拟机
cd ～/workspace/cbdb/deploy/vagrant
vagrant up

##### 待虚拟机启动并自动编译部署完成后，进入虚机
vagrant ssh
psql postgres

##### regression测试
cd ~/workspace/cbdb_dev
make installcheck-good


##### 2.通过docker容器方式部署编译测试环境

##### docker仓库配置
在docker工具中配置公司二进制代码库地址, 通过Docker工具中Preferences->Docker Engine中增加下列JSON格式的配置项目。
"insecure-registries": [
    "docker.artifactory.hashdata.xyz"
]

##### 启动docker容器
cd ～/workspace/cbdb/deploy/docker
bash docker_start.sh

##### 待虚拟机启动并自动编译部署完成后，进入Container, 其中ContainerId可以通过docker ps -a命令查看。
docker exec -it [ContainerId] /bin/bash
psql postgres

##### regression测试
cd ~/workspace/cbdb_dev
make installcheck-good

<br>


### 脚本介绍

cbdb项目的构建和测试环境依赖于很多系统配置和第三方依赖，因此手动部署过程非常繁琐且容易出错。为此，利用vagrant或docker等虚拟化技术引入了cbdb自动化部署编译测试环境，可用于简化用户部署cbdb编译测试环境的工作量和时间成本，让cbdb 开发人员可以更加专注于他们的业务代码开发工作。 cbdb项目支持两种自动化部署方式：第一种是用vagrant部署vbox虚拟机，另一种是用docker部署容器镜像。开发者可以根据自己的需求选择自动化部署的方式。整个自动化部署构建和测试流程都运行在由vagrant或docker管理的虚拟化环境中。通过共享文件机制，宿主机的cbdb代码库被映射到虚拟化环境中的指定路径中，并进行cbdb自动化构建测试。

#### 整个脚本分为三个组成部分：

#### 1.在虚拟化环境中运行的自动化部署脚本
#####  通过vagrant Vagrantfile中provision或者docker Dockerfile中的ENTRYPOINT提供的机制
  *  提供操作编译环境的子命令务
  *  向虚拟机内部脚本提供必要的配置变量并执行具体操作
#####  通过/code/depoly/cbdb_deploy.sh 文件，传递以下编译选项：
  *  编译选项
  *  部署选项
  *  测试选项
#####  通过/code/depoly/cbdb_env.sh 文件，传递自动化编译测试所需的环境变量参数。

#### 2.vagrant部署脚本
##### Vagrantfile
  *  vagrant虚拟机box远程下载并创建
  *  宿主机cbdb代码路径挂载到虚拟机中的指定路径并配置为只读权限，例如：源代码到/home/gpadmin/workspace/cbdb目录
  *  拷贝源文件目录至/home/gpadmin/workspace/cbdb_dev为实际工作目录
  *  保证登陆用户为gpadmin
##### vagrant_release.sh
  *  脚本用于支持自动打包vagrant box镜像文件。


#### 3.docker部署脚本
##### Dockerfile
  *  docker镜像文件打包
  *  镜像容器服务启动，例如ssh服务
##### docker_deploy,sh
  *   生成ssh公共密钥并配置自动登录
  *   启动docker自动部署
##### docker_start.sh
  *  远程下载docker镜像版本
  *  宿主机cbdb代码路径挂载到虚拟机中的指定路径并配置为只读权限，例如：源代码到/home/gpadmin/workspace/cbdb目录
  *  拷贝源文件目录至/home/gpadmin/workspace/cbdb_dev为实际工作目录、
  *  保证登陆用户为gpadmin


