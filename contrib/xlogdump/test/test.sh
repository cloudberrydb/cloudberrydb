PGBIN=/usr/local/pgsql/bin
echo "Creating test cluster... "
mkdir testdata
chmod 700 testdata
$PGBIN/initdb -D testdata > log/test.log 2>&1
echo "Starting test cluster... "
$PGBIN/pg_ctl -l log/backend.log -D testdata start >> log/test.log 2>&1
sleep 2
echo "Inserting test data... "
$PGBIN/psql postgres < sql/test.sql >> log/test.log 2>&1
echo "========================================="
echo "Testing plain dump... "
../xlogdump -T testdata/pg_xlog/000000010000000000000000 > results/dump.result
if [ "`diff results/dump.result expected/dump.result`" = "" ];
then
	echo "Test passed"
else
	echo "Test failed!"
fi
echo "========================================="
echo "Testing transactions dump..."
../xlogdump -t testdata/pg_xlog/000000010000000000000000 > results/transactions.result
if [ "`diff results/transactions.result expected/transactions.result`" = "" ];
then
	echo "Test passed"
else
	echo "Test failed!"
fi
echo "========================================="
echo "Testing dump with translated names..."
../xlogdump -T -h localhost testdata/pg_xlog/000000010000000000000000 > results/names.result
if [ "`diff results/names.result expected/names.result`" = "" ];
then
	echo "Test passed"
else
	echo "Test failed!"
fi
echo "========================================="
echo "Stoping test cluster... "
$PGBIN/pg_ctl -D testdata stop >> log/test.log 2>&1
echo "Removing test cluster... "
rm -rf testdata
echo "Done!"
