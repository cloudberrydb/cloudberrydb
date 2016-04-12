import imp
import os

from mock import *

from gp_unittest import *


class GpCheckCatTestCase(GpTestCase):
    def setUp(self):
        # because gpcheckcat does not have a .py extension, we have to use imp to import it
        # if we had a gpcheckcat.py, this is equivalent to:
        #   import gpcheckcat
        #   self.subject = gpcheckcat
        gpcheckcat_file = os.path.abspath(os.path.dirname(__file__) + "/../../../gpcheckcat")
        self.subject = imp.load_source('gpcheckcat', gpcheckcat_file)

        self.subject.logger = Mock(spec=['log', 'info', 'debug'])
        self.db_connection = Mock(spec=[])

        self.unique_index_violation_check = Mock(spec=['runCheck'])
        self.unique_index_violation_check.runCheck.return_value = []

        # MagicMock: we are choosing to trust the implementation of GV.cfg
        # If we wanted full coverage we would make this a normal Mock()
        # and fully define its behavior
        self.subject.GV.cfg = MagicMock()
        self.subject.GV.checkStatus = True

        self.apply_patches([
            patch("gpcheckcat.pg.connect", return_value=self.db_connection),
            patch("gpcheckcat.UniqueIndexViolationCheck", return_value=self.unique_index_violation_check),
        ])

    def test_running_unknown_check__raises_exception(self):
        with self.assertRaises(LookupError):
            self.subject.runOneCheck('some_unknown_check')

    # @skip("order of checks")
    # def test_run_all_checks__runs_all_checks_in_correct_order(self):
    #     self.subject.runAllChecks()
    #
    #     self.unique_index_violation_check.runCheck.assert_any_call(self.db_connection)
    #     # add other checks here
    #     # figure out how to enforce the order of calls;
    #     # at a minimum, check the order number of the static list gpcheckcat.all_checks

    def test_running_unique_index_violation_check__makes_the_check(self):
        self.subject.runOneCheck('unique_index_violation')

        self.unique_index_violation_check.runCheck.assert_called_with(self.db_connection)

    def test_running_unique_index_violation_check__when_no_violations_are_found__passes_the_check(self):
        self.subject.runOneCheck('unique_index_violation')

        self.assertEqual(self.subject.GV.checkStatus, True)

    def test_running_unique_index_violation_check__when_violations_are_found__fails_the_check(self):
        self.unique_index_violation_check.runCheck.return_value = [
            dict(table_oid=123, table_name='stephen_table', index_name='finger', segment_id=8),
            dict(table_oid=456, table_name='larry_table', index_name='stock', segment_id=-1),
        ]

        self.subject.runOneCheck('unique_index_violation')

        self.assertEqual(self.subject.GV.checkStatus, False)

    def test_checkcat_report__after_running_unique_index_violations_check__reports_violations(self):
        self.unique_index_violation_check.runCheck.return_value = [
            dict(table_oid=123, table_name='stephen_table', index_name='finger', segment_id=8),
            dict(table_oid=456, table_name='larry_table', index_name='stock', segment_id=-1),
        ]
        self.subject.runOneCheck('unique_index_violation')

        self.subject.checkcatReport()

        expected_message1 = 'Table stephen_table on segment 8 has a violated unique index: finger'
        expected_message2 = 'Table larry_table on segment -1 has a violated unique index: stock'
        log_messages = [args[0][1] for args in self.subject.logger.log.call_args_list]
        self.assertIn(expected_message1, log_messages)
        self.assertIn(expected_message2, log_messages)

    def test_drop_leaked_schemas__drops_orphaned_and_leaked_schemas(self):
        self.db_connection.mock_add_spec(['close', 'query'])
        self.subject.getLeakedSchemas = Mock(return_value=["fake_leak_1", "fake_leak_2"])

        self.subject.dropLeakedSchemas(dbname="fake_db")

        drop_query_expected_list = [call('DROP SCHEMA IF EXISTS \"fake_leak_1\" CASCADE;\n'),
                                    call('DROP SCHEMA IF EXISTS \"fake_leak_2\" CASCADE;\n')]
        self.db_connection.query.assert_has_calls(drop_query_expected_list)


if __name__ == '__main__':
    run_tests()
