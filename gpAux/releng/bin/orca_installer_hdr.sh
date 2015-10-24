#!/bin/bash
cat <<-EOF
	## ======================================================================
	##   ______ ______ _____   ______      _____                  
	##  / _____|_____ (____ \ (____  \    / ___ \                 
	## | /  ___ _____) )   \ \ ____)  )  | |   | | ____ ____ ____ 
	## | | (___)  ____/ |   | |  __  (   | |   | |/ ___) ___) _  |
	## | \____/| |    | |__/ /| |__)  )  | |___| | |  ( (__( ( | |
	##  \_____/|_|    |_____/ |______/    \_____/|_|   \____)_||_|
	## ----------------------------------------------------------------------
	## GPDB Orca Optimizer installer
	## ----------------------------------------------------------------------
	## GPDB build number ..... : %%BUILD_NUMBER%%
	## GPDB build timestamp .. : %%PULSE_BUILD_TIMESTAMP%%
	## 
	## Version info:
	##   GPDB ............. : %%GPDB_VER%%
	##   ORCA Optimizer ... : %%OPTIMIZER_VER%%
	##   GPOS  ............ : %%LIBGPOS_VER%%
	##   Xerces ........... : %%XERCES_VER%%
	## ======================================================================

EOF

GPOPTUTILS_NAMESPACE=gpoptutils
PGDATABASE=template1
SKIP=`awk '/^__END_HEADER__/ {print NR + 1; exit 0; }' "$0"`

## ----------------------------------------------------------------------
## Check if GPHOME is set.  If not, inform user and request for them
## to source greenplum_path.sh.
## ----------------------------------------------------------------------
##

if [ -z "${GPHOME}" ]; then
	cat <<-EOF
		FATAL: GPHOME is not set.
		
		  Please source the greenplum_path.sh file before running this script.
	EOF
    exit 1
fi

## ----------------------------------------------------------------------
## Check if current user can contact GPDB instance.
## ----------------------------------------------------------------------

psql ${PGDATABASE} -c 'select version()' > /dev/null 2>&1
if [ $? != 0 ]; then
	cat <<-EOF
		FATAL: Cannot contact GPDB instance.
		
		  Please start instance and/or set the PGPORT environment
		  variable corresponding to your GPDB instance.
	EOF
    exit 1
fi

## ----------------------------------------------------------------------
## Extract GPDB Orca files
## ----------------------------------------------------------------------

if [ ! -d ${GPHOME}/share/orca ]; then
    mkdir -p ${GPHOME}/share/orca
fi

cat <<-EOF
	Extracting GPDB Orca files into work area: ${GPHOME}/share/orca

EOF

tail -n +${SKIP} "$0" | tar zxf - -C ${GPHOME}/share/orca
if [ $? != 0 ]; then
	cat <<-EOF
		FATAL: Extraction failed.
	EOF
    exit 1
fi

LOAD_ORCA_SQL=${GPHOME}/share/orca/load.sql.in

if [ $( uname -s ) = "Darwin" ]; then
	LDSFX=dylib
else
	LDSFX=so
fi

if [ ! -d ${GPHOME}/share/orca ]; then
	cat <<-EOF
		FATAL: Cannot access orca directory (${GPHOME}/share/orca).
	EOF
    exit 1
fi

cat <<-EOF
	Setup symlinks in lib directory: ${GPHOME}/lib

EOF
for library in $( ls ${GPHOME}/share/orca/lib* ); do
	rm -f ${GPHOME}/lib/$( basename $library )
	echo "  ln -s ${GPHOME}/share/orca/$library ${GPHOME}/lib"
	ln -s $library ${GPHOME}/lib
done

echo ""

##
## Prepare ORCA UDF SQL commands
##

cat <<-EOF
	Prepare GPDB Orca UDF SQL file: $( dirname ${LOAD_ORCA_SQL} )/$( basename ${LOAD_ORCA_SQL} .in)

EOF
rm -f $( dirname ${LOAD_ORCA_SQL} )/$( basename ${LOAD_ORCA_SQL} .in)
sed -e "s|%%CURRENT_DIRECTORY%%|${GPHOME}/lib|g" \
    -e "s|%%GPOPTUTILS_NAMESPACE%%|${GPOPTUTILS_NAMESPACE}|g" \
    -e "s|%%EXT%%|${LDSFX}|g" ${LOAD_ORCA_SQL} > $( dirname ${LOAD_ORCA_SQL} )/$( basename ${LOAD_ORCA_SQL} .in)

cat <<-EOF
	Executing GPDB Orca UDF SQL file: $( dirname ${LOAD_ORCA_SQL} )/$( basename ${LOAD_ORCA_SQL} .in)

EOF
psql ${PGDATABASE} -a -f $( dirname ${LOAD_ORCA_SQL} )/$( basename ${LOAD_ORCA_SQL} .in)
if [ $? != 0 ]; then
	cat <<-EOF
		FATAL: Orca Optimizer UDF SQL failed.
		
		  Please review output.
	EOF
    exit 1
fi

cat <<-EOF

	## ======================================================================
	## GPDB Orca Optimizer installation is complete
	## Timestamp: $( date )
	## ======================================================================
EOF

exit 0

__END_HEADER__
