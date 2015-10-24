Summary:        Data Domain Boost libraries for Greenplum Database 
License:        EMC Copyright
Name:           ddboostlib
Version:        %{ddboostlib_ver}
Release:        %{ddboostlib_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       ddboostlib = %{ddboostlib_ver}

%description
The DDBoostlib package updates the Data Domain services for the Greenplum database.

%install
mkdir -p %{buildroot}/temp/lib
cp -rf %{ddboostlib_dir}/../../../../../gpAux/ext/%{bld_arch}/lib/libDDBoost.so %{buildroot}/temp/lib/

%files
/temp
