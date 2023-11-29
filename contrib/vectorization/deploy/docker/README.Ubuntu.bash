#!/bin/bash
pip3 install conan

tee -a /etc/sysctl.conf << EOF
kernel.shmmax = 5000000000000
kernel.shmmni = 32768
kernel.shmall = 40000000000
kernel.sem = 1000 32768000 1000 32768
kernel.msgmnb = 1048576
kernel.msgmax = 1048576
kernel.msgmni = 32768

net.core.netdev_max_backlog = 80000
net.core.rmem_default = 2097152
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216

vm.overcommit_memory = 2
vm.overcommit_ratio = 95
EOF

sysctl -p

mkdir -p /etc/security/limits.d
tee -a /etc/security/limits.d/90-greenplum.conf << EOF
* soft nofile 1048576
* hard nofile 1048576
* soft nproc 1048576
* hard nproc 1048576
EOF

echo "/home/gpadmin/crash/coredump.%e.%p" > /proc/sys/kernel/core_pattern
ulimit -n 65536 65536