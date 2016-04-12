import imp
import os
from mock import *
from gp_unittest import *


class GpCheckCatTestCase(GpTestCase):
    def setUp(self):
        # because gpcheckcat does not have a .py extension, we have to use imp to import it
        # if we had a gpcheckcat.py, this is equivalent to:
        #   from lib import gpcheckcat
        #   self.subject = gpcheckcat
        gpcheckcat_path = os.path.abspath('lib/gpcheckcat')
        self.subject = imp.load_source('gpcheckcat', gpcheckcat_path)

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

    @skip("order of checks")
    def test_runAllChecks_runsAllChecksInCorrectOrder(self, check):
        self.subject.runAllChecks()
        self.unique_index_violation_check.runCheck.assert_any_call(self.db_connection)
        # add other checks here
        # figure out how to enforce the order of calls;
        # at a minimum, check the order number of the static list gpcheckcat.all_checks

    def test_runningUnknownCheck_raisesException(self):
        with self.assertRaises(LookupError):
            self.subject.runOneCheck('some_unknown_check')

    def test_runningUniqueIndexViolationCheck_makesTheCheck(self):
        self.subject.runOneCheck('unique_index_violation')

        self.unique_index_violation_check.runCheck.assert_called_with(self.db_connection)

    def test_runningCheck_whenNoViolationsAreFound_passesTheCheck(self):
        self.subject.runOneCheck('unique_index_violation')

        self.assertEqual(self.subject.GV.checkStatus, True)

    def test_runningCheck_whenViolationsAreFound_failsTheCheck(self):
        self.unique_index_violation_check.runCheck.return_value = [
            dict(table_oid=123, table_name='stephen_table', index_name='finger', segment_id=8),
            dict(table_oid=456, table_name='larry_table', index_name='stock', segment_id=-1),
        ]

        self.subject.runOneCheck('unique_index_violation')

        self.assertEqual(self.subject.GV.checkStatus, False)

    def test_checkcatReport_afterRunningUniqueIndexViolationCheck_reportsViolations(self):
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

if __name__ == '__main__':
    run_tests()
