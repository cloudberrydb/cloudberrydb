Summary:        PL/Perl for Greenplum Database 
License:        Artistic License        
Name:           plperl
Version:        %{plperl_ver}
Release:        %{plperl_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       plperl = %{plperl_ver} 

%description
The PL/Perl package provides Procedural language implementation of Perl for Greenplum Database.

%install
mkdir -p %{buildroot}/temp
make -C %{plperl_dir} install prefix=%{buildroot}/temp

%files
/temp
