--\! source /Users/simiob/Perforce/cdbfast/private/simiob/cdb2/gpdemo/JDBC/greenplum_connectivity_path.sh
--\! source ../../../../gpdemo/JDBC/greenplum_connectivity_path.sh
--\! export PGUSER=`whoami`
--\! export PGHOST=localhost

\! javac data/jdbc/*.java
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ SimplePostgresJDBC | tee -a data/jdbc/SimplePostgresJDBC.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ SimplePostgresSQL | tee -a data/jdbc/SimplePostgresSQL.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ query03 | tee -a data/jdbc/query03.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ query04 | tee -a data/jdbc/query04.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ query05 | tee -a data/jdbc/query05.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ query06 | tee -a data/jdbc/query06.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ query07 | tee -a data/jdbc/query07.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ query08 | tee -a data/jdbc/query08.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ query09 | tee -a data/jdbc/query09.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ query11 | tee -a  data/jdbc/query11.out
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ mpp17727 | tee -a data/jdbc/mpp17727.out
\! echo "============================================="
\! psql -f data/jdbc/mpp12490.sql template1
\! echo "============================================="
\! java -cp $CLASSPATH:./data/jdbc/ mpp12490 | tee -a  data/jdbc/mpp12490.out
\! echo "============================================="

\! rm -f data/jdbc/*.class

