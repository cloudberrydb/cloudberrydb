Summary:        Proj library 
License:        MIT License        
Name:           proj
Version:        %{proj_ver}
Release:        %{proj_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       proj = %{proj_ver} 

%description
The Proj module provides cartographic projections library which is used by PostGIS.

%install
mkdir -p %{buildroot}/temp/lib
cp -rf %{proj_dir}/lib/libproj* %{buildroot}/temp/lib

%files
/temp
