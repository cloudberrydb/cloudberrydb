Summary:        PL/R for greenplum database 
License:        GPLv2       
Name:           plr
Version:        %{plr_ver}
Release:        %{plr_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       plr = %{plr_ver}
Requires:       R = %{r_ver}

%description
The PL/R modules provides Procedural language implementation of R for Greenplum Database.

%install
mkdir -p %{buildroot}/temp
make -C %{plr_dir} install DESTDIR=%{buildroot}/temp bindir=/bin libdir=/lib/postgresql pkglibdir=/lib/postgresql datadir=/share/postgresql R_HOME=%{r_dir}/lib64/R USE_PGXS=1

%files
/temp
