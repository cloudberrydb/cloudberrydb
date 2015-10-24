Summary:        Perl 
License:        Artistic License        
Name:           perl
Version:        %{perl_ver}
Release:        %{perl_rel} 
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       perl = %{perl_ver}, /bin/sh 

%description
The Perl package provides Perl for PL/Perl. 

%install
mkdir -p %{buildroot}/temp/ext/perl-%{perl_ver}
cp -r %{perl_dir}/*   %{buildroot}/temp/ext/perl-%{perl_ver}

%post
echo "PERL5HOME=\$GPHOME/ext/perl-%{perl_ver}" >> $GPHOME/greenplum_path.sh
echo "export PATH=\$PERL5HOME/bin:\$PATH" >> $GPHOME/greenplum_path.sh
echo "export LD_LIBRARY_PATH=\$PERL5HOME/lib/%{perl_ver}/x86_64-linux/CORE:\$LD_LIBRARY_PATH" >> $GPHOME/greenplum_path.sh
echo "export PERL5LIB=\$PERL5HOME/lib/site_perl/%{perl_ver}/x86_64-linux:\$PERL5HOME/lib/site_perl/%{perl_ver}:\$PERL5HOME/lib/%{perl_ver}/x86_64-linux:\$PERL5HOME/lib/%{perl_ver}" >> $GPHOME/greenplum_path.sh

%postun
sed -i".bk" "/PERL5HOME/d" $GPHOME/greenplum_path.sh
rm -rf $GPHOME/greenplum_path.sh.bk

%files
/temp
