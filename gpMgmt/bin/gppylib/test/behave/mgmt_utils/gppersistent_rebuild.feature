@gppersistent_rebuid
  Feature: persistent rebuild tests

  Scenario: Persistent rebuild tools should not error out when mirror is marked down and files are missing in the data directory for single node
    Given the database is running
    And the information of a "mirror" segment on any host is saved
    When user kills a mirror process with the saved information
    And user temporarily moves the data directory of the killed mirror
    And wait until the mirror is down
    Then run gppersistent_rebuild with the saved content id
    And gppersistent_rebuild should return a return code of 0
    And the user runs command "$GPHOME/bin/lib/gpcheckcat -R persistent -A"
    And gpcheckcat should return a return code of 0
    And user returns the data directory to the default location of the killed mirror
    And the user runs command "gprecoverseg -a"
    And gprecoverseg should return a return code of 0
