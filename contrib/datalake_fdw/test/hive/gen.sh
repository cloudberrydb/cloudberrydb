#!/bin/bash
PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/conf.sh 

function generate_vast_data() {
	normal_file="$PWD/hive_data/vast_normal.sql"
	partition_file="$PWD/hive_data/vast_partition.sql"
    cat <<- EOF > ${normal_file}
	DROP DATABASE IF EXISTS ${HIVE_NORMAL_DB} CASCADE;
	CREATE DATABASE ${HIVE_NORMAL_DB};
	USE ${HIVE_NORMAL_DB};
	CREATE TABLE normal_orc_1
	(
		a tinyint,
		b smallint,
		c int,
		d bigint,
		e float,
		f double,
		g string,
		h timestamp,
		i date,
		j char(20),
		k varchar(20),
		l decimal(20, 10)
	) STORED AS ORC;
	EOF

    cat <<- EOF > ${partition_file}
	DROP DATABASE IF EXISTS ${HIVE_PARTITION_DB} CASCADE;
	CREATE DATABASE ${HIVE_PARTITION_DB};
	USE ${HIVE_PARTITION_DB};
	CREATE TABLE partition_orc_1
	(
		a tinyint,
		b smallint,
		c int,
		d bigint,
		e float,
		f double,
		g string,
		h timestamp,
		i date,
		j char(20),
		k varchar(20),
		l decimal(20, 10)
	)
	PARTITIONED BY
	(
		m int,
		n varchar(20)
	)
	STORED AS ORC;
	EOF

    for i in $(seq 2 $NORMAL_COUNT); do
	    cat <<- EOF >> ${normal_file}
		CREATE TABLE normal_orc_${i} like normal_orc_1;
		EOF
    done

    for i in $(seq 2 $PARTITION_COUNT); do
	    cat <<- EOF >> ${partition_file}
		CREATE TABLE partition_orc_${i} like partition_orc_1;
		EOF
    done
}

function generate_val_file() {
    test_file="$PWD/sql/valgrind.sql"
    echo "" > $test_file
    for tab in $ALL_TAB; do
        cat <<- EOF >> $test_file
		SELECT public.sync_hive_table('$HIVE_CLUSTER','$HIVE_DB','$tab','$HDFS_CLUSTER', '$SCHEMA.$tab', '$SIMP_SERVER');

		EOF
    done
    echo "SELECT public.sync_hive_database('$HIVE_CLUSTER', '$HIVE_DB', '$HDFS_CLUSTER', '$DB_SCHEMA', '$SIMP_SERVER');" >> $test_file
    for tab in $ALL_TAB; do
        cat <<- EOF >> $test_file
		\d $DB_SCHEMA.$tab
		SELECT * FROM $SCHEMA.$tab;

		EOF
    done
}

if [[ $GEN_VAL == "true" ]]; then
    echo "Generating sync test files..."
    generate_val_file
fi

if [[ $GEN_VAST == "true" ]]; then
    echo "Generating vast data files..."
    generate_vast_data
fi
