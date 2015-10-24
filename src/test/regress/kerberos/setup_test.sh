#!/bin/bash

if [ -z "$MASTER_DATA_DIRECTORY" ]; then
	echo "test setup_kerberos_master_data_dir ... FAILED (0 sec)"
	exit 1
fi

if [ -z "$PGPORT" ]; then
	echo "test setup_kerberos_pgport ... FAILED (0 sec)"
	exit 1
fi

kerb_host=`hostname`
TestConnectivity() {
    CONNECTIVITY_RESULT=`psql -U "gpadmin/kerberos-test" -h ${kerb_host} template1 -c "select version()" | grep PostgreSQL | awk '{ print $1 }'`
}


psql template1 -c 'create role "gpadmin/kerberos-test" login superuser'

if ! [ -e $MASTER_DATA_DIRECTORY/pg_hba.conf.orig ]; then
    cp $MASTER_DATA_DIRECTORY/pg_hba.conf $MASTER_DATA_DIRECTORY/pg_hba.conf.orig
fi

cat $MASTER_DATA_DIRECTORY/pg_hba.conf.orig  | sed  -e 's/local.*trust/#removed insecure line/' | sed -e 's/host.*0\.0\.0\.0.*trust/#removed insecure line/' > $MASTER_DATA_DIRECTORY/pg_hba.conf
echo "host    all     all     0.0.0.0/0       gss include_realm=0 krb_realm=KRB.GREENPLUM.COM" >> $MASTER_DATA_DIRECTORY/pg_hba.conf
echo "local   all     gpadmin/kerberos-test   trust" >> $MASTER_DATA_DIRECTORY/pg_hba.conf


echo "krb_server_keyfile='$HOME/gpdb-kerberos.keytab'" >> $MASTER_DATA_DIRECTORY/postgresql.conf

gpstop -ra

LD_LIBRARY_PATH= kdestroy

TestConnectivity
if [ -z "$CONNECTIVITY_RESULT" ]; then
	echo "test setup_kerberos_kdestroy ... ok (0 sec)"
else
	echo "RESULT: $RESULT"
	echo "test setup_kerberos_kdestroy ... FAILED (0 sec)"
	exit 1
fi

LD_LIBRARY_PATH= kinit -k -t $HOME/gpdb-kerberos.keytab gpadmin/kerberos-test@KRB.GREENPLUM.COM

TestConnectivity
if [ -z "$CONNECTIVITY_RESULT" ]; then
	echo "psql did not return postgres version"
	echo "test setup_kerberos_kinit ... FAILED (0 sec)"
	exit 1

elif [ "$CONNECTIVITY_RESULT" == "PostgreSQL" ]; then
	echo "test setup_kerberos_kinit ... ok (0 sec)"

else
	echo "RESULT: $CONNECTIVITY_RESULT"
	echo "test setup_kerberos_kinit ... FAILED (0 sec)"
	exit 1
fi

