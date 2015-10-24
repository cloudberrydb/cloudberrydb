Feature: Scale tests for backup

    @dca
    @backupscale
    Scenario: Test restore for large number of tables
         Given the database is running
         And the database "large_table_db" does not exist
         And database "large_table_db" exists
         And there is a "ao" table "ao_table_for_large_database_01" in "large_table_db" having large number of partitions
         And the length of partition names of table "ao_table_for_large_database_01" in "large_table_db" exceeds the command line maximum limit
         And there are no backup files
         When the performance timer is started
         When the user runs "gpcrondump -a -x large_table_db"
         Then the performance timer should be less then "120" seconds
         And gpcrondump should return a return code of 0
         When the performance timer is started
         And the user runs "gpcrondump -a -x large_table_db --incremental"
         Then the performance timer should be less then "140" seconds
         And gpcrondump should return a return code of 0
         And the timestamp from gpcrondump is stored 
         And all the data from "large_table_db" is saved for verification
         When the performance timer is started
         And the user runs gpdbrestore with the stored timestamp 
         Then the performance timer should be less then "1100" seconds
         Then gpdbrestore should return a return code of 0
         And verify that the data of "5551" tables in "large_table_db" is validated after restore 
         And verify that the tuple count of all appendonly tables are consistent in "large_table_db"

    @customer
    @backupscale
    Scenario: gpcrondump -t with parent partition for large partition tables
        Given the database is running
        And the database "large_table_db" does not exist
        And database "large_table_db" exists
        And there is a "ao" table "ao_table" with compression "None" in "large_table_db" with data
        And there is a "ao" table "ao_table_for_large_database_01" in "large_table_db" having large number of partitions
        And the length of partition names of table "ao_table_for_large_database_01" in "large_table_db" exceeds the command line maximum limit
        And there are no backup files
        When the user runs "gpcrondump -a -x large_table_db -t public.ao_table_for_large_database_01"
        And gpcrondump should return a return code of 0
        Then gpcrondump should print --table-file=/tmp/include_dump_tables_file to stdout
        And gpcrondump should not print --table="public.ao_table_for_large_database_01" to stdout
        And all the data from "large_table_db" is saved for verification
        And the timestamp from gpcrondump is stored 
        And the user runs gpdbrestore with the stored timestamp 
        Then gpdbrestore should return a return code of 0
        And verify that the data of "5551" tables in "large_table_db" is validated after restore 
        
    @customer
    @backupscale
    Scenario: gpcrondump --table-file with parent partition for large partition tables
        Given the database is running
        And the database "large_table_db" does not exist
        And database "large_table_db" exists
        And there is a "ao" table "ao_table" with compression "None" in "large_table_db" with data
        And there is a "ao" table "ao_table_for_large_database_01" in "large_table_db" having large number of partitions
        And the length of partition names of table "ao_table_for_large_database_01" in "large_table_db" exceeds the command line maximum limit
        And there are no backup files
        And there is a file "include_file" with tables "public.ao_table_for_large_database_01" 
        When the user runs "gpcrondump -a -x large_table_db --table-file include_file"
        And gpcrondump should return a return code of 0
        Then gpcrondump should print --table-file=/tmp/include_dump_tables_file to stdout
        And gpcrondump should not print --table="public.ao_table_for_large_database_01" to stdout
        And the timestamp from gpcrondump is stored 
        And all the data from "large_table_db" is saved for verification
        And the user runs gpdbrestore with the stored timestamp 
        Then gpdbrestore should return a return code of 0
        And verify that the data of "5551" tables in "large_table_db" is validated after restore 

    @customer
    @backupscale
    Scenario: gpcrondump -T with parent partition for large partition tables
        Given the database is running
        And the database "large_table_db" does not exist
        And database "large_table_db" exists
        And there is a "ao" table "ao_table" with compression "None" in "large_table_db" with data
        And there is a "ao" table "ao_table_for_large_database_01" in "large_table_db" having large number of partitions
        And the length of partition names of table "ao_table_for_large_database_01" in "large_table_db" exceeds the command line maximum limit
        And there are no backup files
        When the user runs "gpcrondump -a -x large_table_db -T public.ao_table_for_large_database_01"
        And gpcrondump should return a return code of 0
        Then gpcrondump should print --exclude-table-file=/tmp/exclude_dump_tables_file to stdout
        And gpcrondump should not print --exclude-table="public.ao_table_for_large_database_01" to stdout
        And the timestamp from gpcrondump is stored 
        And all the data from "large_table_db" is saved for verification
        And the user runs gpdbrestore with the stored timestamp 
        Then gpdbrestore should return a return code of 0
        And verify that the data of "1" tables in "large_table_db" is validated after restore 
        And verify that the tuple count of all appendonly tables are consistent in "large_table_db"

    @customer
    @backupscale
    Scenario: gpcrondump --exclude-table-file with parent partition for large partition tables
        Given the database is running
        And the database "large_table_db" does not exist
        And database "large_table_db" exists
        And there is a "ao" table "ao_table" with compression "None" in "large_table_db" with data
        And there is a "ao" table "ao_table_for_large_database_01" in "large_table_db" having large number of partitions
        And the length of partition names of table "ao_table_for_large_database_01" in "large_table_db" exceeds the command line maximum limit
        And there are no backup files
        And there is a file "exclude_file" with tables "public.ao_table_for_large_database_01" 
        When the user runs "gpcrondump -a -x large_table_db --exclude-table-file exclude_file"
        And gpcrondump should return a return code of 0
        Then gpcrondump should print --exclude-table-file=/tmp/exclude_dump_tables_file to stdout
        And gpcrondump should not print --exclude-table="public.ao_table_for_large_database_01" to stdout
        And the timestamp from gpcrondump is stored 
        And all the data from "large_table_db" is saved for verification
        And the user runs gpdbrestore with the stored timestamp 
        Then gpdbrestore should return a return code of 0
        And verify that the data of "1" tables in "large_table_db" is validated after restore 
        And verify that the tuple count of all appendonly tables are consistent in "large_table_db"

    @irs
    @customer
    @backupscaleextreme
    Scenario: Test full backup and restore for very large number of tables 
        Given the database is running
        And the database "scale_db" does not exist
        And database "scale_db" exists
        And there are no backup files
        When the user runs "psql -f gppylib/test/behave/mgmt_utils/steps/data/tables.sql scale_db"
        And the performance timer is started
        And the user runs "gpcrondump -a -x scale_db"
        Then gpcrondump should return a return code of 0 
        And the performance timer should be less then "10000" seconds
        And the timestamp from gpcrondump is stored 
        When the performance timer is started
        And the user runs gpdbrestore with the stored timestamp
        Then gpdbrestore should return a return code of 0
        And the performance timer should be less then "45000" seconds
