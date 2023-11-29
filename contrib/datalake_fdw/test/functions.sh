function create_ext(){
    db=$1
    psql -d ${db} -c "DROP EXTENSION IF EXISTS datalake_fdw CASCADE"
    psql -d ${db} -c "DROP EXTENSION IF EXISTS hive_connector"
    psql -d ${db} -c "CREATE EXTENSION hive_connector"
    psql -d ${db} -c "CREATE EXTENSION datalake_fdw"
    psql -d ${db} -c "CREATE FOREIGN DATA WRAPPER datalake_fdw HANDLER datalake_fdw_handler VALIDATOR datalake_fdw_validator OPTIONS ( mpp_execute 'all segments' )"
}

function create_schema() {
    db=$1
    schema=$2
    psql -d $db -c "drop schema if exists $schema cascade"
    psql -d $db -c "create schema $schema"
}

function create_server(){
    db=$1
    server=$2
    hdfs_cluster=$3
    psql -d ${db} -c "DROP SERVER IF EXISTS ${server} CASCADE"
    psql -d ${db} -c "SELECT public.create_foreign_server('${server}', 'gpadmin', 'datalake_fdw', '${hdfs_cluster}')"
}

function create_oss_server(){
    db=$1
    server=$2
    psql -d ${db} -c "DROP SERVER IF EXISTS ${server} CASCADE"
    psql -d ${db} -c "CREATE SERVER ${server} FOREIGN DATA WRAPPER datalake_fdw OPTIONS (host 'pek3b.qingstor.com', protocol 'qs', isvirtual 'false', ishttps 'false')"
    psql -d ${db} -c "CREATE USER MAPPING FOR gpadmin SERVER ${server} OPTIONS (user 'gpadmin', accesskey 'KGCPPHVCHRMZMFEAWLLC', secretkey '0SJIWiIATh6jOlmAKr8DGq6hOAGBI1BnsnvgJmTs');"
}

function valgrind_check(){
    command="valgrind --leak-check=full --show-leak-kinds=all ${command}"
}

function get_time_elapse(){
    seconds=$1
    hour=$(( $seconds/3600 ))
    min=$(( ($seconds-${hour}*3600)/60 ))
    sec=$(( $seconds-${hour}*3600-${min}*60 ))
    HMS="${hour}h:${min}m:${sec}s"
}

function psql_exec_print_time(){
    command=$1
    msg=$2
    time_start=$(date +%s)
    eval "$command"
    time_end=$(date +%s)
    get_time_elapse $(( $time_end - $time_start ))
    echo "Time cost to $msg: $HMS"
}
