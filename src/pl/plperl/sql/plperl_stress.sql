--Test to return large scale data over a table with large number of rows,
--and each result set is of different size.    
CREATE TABLE test (a int) DISTRIBUTED RANDOMLY;


CREATE TABLE table10000 AS SELECT * from generate_series(1,10000) DISTRIBUTED RANDOMLY;


-- Create Function to return setof random number of integers 
--
CREATE OR REPLACE FUNCTION setof_int()
RETURNS SETOF INTEGER AS $$
    my $range = 20000;
    my $random_number = int(rand($range));
    foreach (1..$random_number) {
        return_next(1);
    }
    return undef;
$$ LANGUAGE plperl;


--(1) Return " setof integer " with ten thousands of tuplestores and each tuplestore containing  random number(1…20000) of integers, 
--    so totally handle about 400 Megabytes. 
CREATE TABLE setofIntRes AS SELECT setof_int() from table10000 DISTRIBUTED RANDOMLY;
DROP TABLE setofIntRes;


DROP FUNCTION setof_int();


--Create Function to return setof random number of rows 
--
CREATE OR REPLACE FUNCTION setof_table_random ()
RETURNS SETOF test AS $$
    my $range = 20000;
    my $random_number = int(rand($range));
    foreach (1..$random_number) {
        return_next({a=>1});
    }
    return undef;
$$ LANGUAGE plperl;


--(2) Return "setof table" with ten thousands of tuplestores and each tuplestore containing random number(1…20000) of rows(each row just has one int 
--    column),so totally handle about  400 Megabytes.
CREATE TABLE setofTableRes AS SELECT setof_table_random() from table10000 DISTRIBUTED RANDOMLY;
DROP TABLE setofTableRes;


DROP FUNCTION setof_table_random ();

DROP TABLE test;

DROP TABLE table10000;
