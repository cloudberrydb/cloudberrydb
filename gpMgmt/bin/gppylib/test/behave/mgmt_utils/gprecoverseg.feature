@gprecoverseg
Feature: gprecoverseg tests
    @dca
    Scenario: gprecoverseg should not output bootstrap error on success
        Given the database is running
        And user kills a primary postmaster process
        And user can start transactions
        When the user runs "gprecoverseg -a"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print Unhandled exception in thread started by <bound method Worker.__bootstrap to stdout
        And the segments are synchronized
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print Unhandled exception in thread started by <bound method Worker.__bootstrap to stdout
         
    @wip
    @dca
    Scenario: Pid corresponds to a non postgres process 
        Given the database is running
        and all the segments are running
        And the segments are synchronized
        and the "primary" segment information is saved
        When the postmaster.pid file on "primary" segment is saved
        And user kills a primary postmaster process
        When user can start transactions
        And the background pid is killed on "primary" segment
        And we run a sample background script to generate a pid on "primary" segment
        And we generate the postmaster.pid file with the background pid on "primary" segment
        And the user runs "gprecoverseg -a" 
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print Unhandled exception in thread started by <bound method Worker.__bootstrap to stdout
        And gprecoverseg should print Skipping to stop segment.*on host.*as it is not a postgres process to stdout
        And all the segments are running
        And the segments are synchronized
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print Unhandled exception in thread started by <bound method Worker.__bootstrap to stdout
        And the segments are synchronized
        And the backup pid file is deleted on "primary" segment
        And the background pid is killed on "primary" segment

    @wip
    @dca
    Scenario: Pid does not correspond to any running process 
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        and the "primary" segment information is saved
        When the postmaster.pid file on "primary" segment is saved
        And user kills a primary postmaster process
        When user can start transactions
        And we generate the postmaster.pid file with a non running pid on the same "primary" segment
        And the user runs "gprecoverseg -a"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print Unhandled exception in thread started by <bound method Worker.__bootstrap to stdout
        And all the segments are running
        And the segments are synchronized
        When the user runs "gprecoverseg -ra"
        Then gprecoverseg should return a return code of 0
        And gprecoverseg should not print Unhandled exception in thread started by <bound method Worker.__bootstrap to stdout
        And the segments are synchronized
        And the backup pid file is deleted on "primary" segment

    @dca
    Scenario: gprecoveseg testing using gpfaultinjector 
        Given the database is running
        And all the segments are running
        And the segments are synchronized
        And the information of a "mirror" segment on a remote host is saved
        And user runs the command "gpfaultinjector  -f filerep_consumer  -m async -y fault" with the saved mirror segment option
        Then the saved mirror segment is marked down in config
        And the saved mirror segment process is still running on that host
        And user can start transactions
        When the user runs "gprecoverseg -F -a"
        Then gprecoverseg should return a return code of 0
        And all the segments are running

