dropdb regression
createdb regression

psql regression -a -f sql/output-test.sql > output-test.out

if diff output-test.out expected/output-test.out; then
    echo Success
else
    echo Fail
fi