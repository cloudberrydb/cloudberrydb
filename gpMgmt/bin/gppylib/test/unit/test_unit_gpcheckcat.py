import imp
import logging
import os
import sys

import mock
import unittest2 as unittest
from mock import patch

gpcheckcat_path = os.path.abspath('lib/gpcheckcat')

class GpCheckCatTestCase(unittest.TestCase):

    def setUp(self):
        self.subject = imp.load_source('gpcheckcat', gpcheckcat_path)
        self.subject.GV.cfg = mock.MagicMock()
        self.subject.logger = mock.Mock()

        self.db_connection = mock.Mock()

    @unittest.skip("order of checks")
    @patch('gpcheckcat.UniqueIndexViolationCheck')
    def test_runAllChecks_runsAllChecksInCorrectOrder(self, check):
        self.subject.runAllChecks()
        check.return_value.runCheck.assert_any_call(self.db_connection)
        # add other checks here
        # figure out how to enforce the order of calls; at a minimum, check the order number of the static list of gpcheckcat.all_checks

    def test_runningUnknownCheck_raisesException(self):
        with self.assertRaises(LookupError):
            self.subject.runOneCheck('some_unknown_check')

    @patch('gpcheckcat.UniqueIndexViolationCheck')
    @patch('gpcheckcat.pg.connect')
    def test_runningUniqueIndexViolationCheck_makesTheCheck(self, connect, check):
        connect.return_value = self.db_connection

        self.subject.runOneCheck('unique_index_violation')

        check.return_value.runCheck.assert_called_with(self.db_connection)

    @patch('gpcheckcat.UniqueIndexViolationCheck')
    @patch('gpcheckcat.pg.connect')
    def test_checkcatReport_afterRunningUniqueIndexViolationCheck_reportsViolations(self, connect, check):
        connect.return_value = self.db_connection

        check.return_value.runCheck.return_value = [
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
    logging.basicConfig(stream=sys.stderr)
    log = logging.getLogger()
    log.setLevel(logging.DEBUG)

    (unittest.main(verbosity=2, buffer=True))