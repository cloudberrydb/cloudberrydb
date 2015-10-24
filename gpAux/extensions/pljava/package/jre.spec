Summary:        Java  
License:        Sun License       
Name:           jre
Version:        %{jre_ver}
Release:        %{jre_rel} 
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       jre = %{jre_ver}, /bin/sh

%description
This package provides JRE %{jre_ver}, needed for PL/Java.

%install
mkdir -p %{buildroot}/temp/ext/jre-%{jre_ver}
cp -r %{jre_dir}/* %{buildroot}/temp/ext/jre-%{jre_ver}

%post
echo "JAVA_HOME=\$GPHOME/ext/jre-%{jre_ver}/jre%{jre_ver}" >> $GPHOME/greenplum_path.sh
echo "export JAVA_HOME" >> $GPHOME/greenplum_path.sh
echo "PATH=\$JAVA_HOME/bin:\$PATH" >> $GPHOME/greenplum_path.sh
echo "export PATH" >> $GPHOME/greenplum_path.sh
echo "LD_LIBRARY_PATH=\$JAVA_HOME/lib/amd64/server:\$LD_LIBRARY_PATH" >> $GPHOME/greenplum_path.sh
echo "export LD_LIBRARY_PATH" >> $GPHOME/greenplum_path.sh

%postun
sed -i".bk" "s|export JAVA_HOME=\$GPHOME/ext/jre-%{jre_ver}/jre%{jre_ver}||g" $GPHOME/greenplum_path.sh
sed -i".bk" "s|export PATH=\$JAVA_HOME/bin:\$PATH||g" $GPHOME/greenplum_path.sh
sed -i".bk" "s|export LD_LIBRARY_PATH=\$JAVA_HOME/lib/amd64/server:\$LD_LIBRARY_PATH||g" $GPHOME/greenplum_path.sh
rm -rf $GPHOME/greenplum_path.sh.bk

%files
/temp
