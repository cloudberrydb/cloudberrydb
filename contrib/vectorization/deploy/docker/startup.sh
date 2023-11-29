#!/bin/bash
user=${1}
parent_dir=${2}
home_dir=/home/${user}

# zsh setup
cp -r /root/.oh-my-zsh /home/gpadmin/.oh-my-zsh 
echo 'git config --global --add oh-my-zsh.hide-status 1' >> /root/.zshrc 
echo 'git config --global --add oh-my-zsh.hide-dirty 1' >> /root/.zshrc 
cp /root/.zshrc /home/gpadmin/.zshrc 
chown -R gpadmin:gpadmin /home/gpadmin/.oh-my-zsh

# disable git status check in zsh
git config --global --add oh-my-zsh.hide-status 1
git config --global --add oh-my-zsh.hide-dirty 1

# modify bashrc
echo "cbdb_dir=\"$parent_dir/cbdb\"" > /home/gpadmin/.bashrc
echo 'export PATH=/home/gpadmin/.local/bin:$GPHOME/bin:$PATH' >> /home/gpadmin/.bashrc
echo 'export PYTHONPATH=/home/gpadmin/install/gpdb/lib/python:$PYTHONPATH' >> /home/gpadmin/.bashrc
echo 'export LD_LIBRARY_PATH=/usr/local/lib/x86_64-linux-gnu:/usr/local/lib:/home/gpadmin/install/gpdb/lib:$LD_LIBRARY_PATH' >> /home/gpadmin/.bashrc
echo 'if [ -f  /home/gpadmin/install/gpdb/greenplum_path.sh ]
then
  source /home/gpadmin/install/gpdb/greenplum_path.sh
fi' >> /home/gpadmin/.bashrc
echo "gpdemo_file=\"\$cbdb_dir/gpAux/gpdemo/gpdemo-env.sh\"" >> /home/gpadmin/.bashrc
echo 'if [ -f "$gpdemo_file" ]; then source "$gpdemo_file"; fi' >> /home/gpadmin/.bashrc
echo 'export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/lib/x86_64-linux-gnu/pkgconfig' >> /home/gpadmin/.bashrc
echo 'export USER=gpadmin' >> /home/gpadmin/.bashrc
echo 'source /home/gpadmin/.bashrc' >> /home/gpadmin/.zshrc

ldconfig

/bin/zsh
