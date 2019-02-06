import unittest
import threading

import mock

from gppylib.commands.base import Command, ExecutionError, WorkerPool

class WorkerPoolTest(unittest.TestCase):
    def setUp(self):
        self.pool = WorkerPool(numWorkers=1, logger=mock.Mock())

    def tearDown(self):
        # All background threads must be stopped, or else the test runner will
        # hang waiting. Join the stopped threads to make sure we're completely
        # clean for the next test.
        self.pool.haltWork()
        self.pool.joinWorkers()

    def test_pool_must_have_some_workers(self):
        with self.assertRaises(Exception):
            WorkerPool(numWorkers=0)
        
    def test_pool_runs_added_command(self):
        cmd = mock.Mock(spec=Command)

        self.pool.addCommand(cmd)
        self.pool.join()

        cmd.run.assert_called_once_with()

    def test_completed_commands_are_retrievable(self):
        cmd = mock.Mock(spec=Command)

        self.pool.addCommand(cmd) # should quickly be completed
        self.pool.join()

        self.assertEqual(self.pool.getCompletedItems(), [cmd])

    def test_pool_is_not_marked_done_until_commands_finish(self):
        cmd = mock.Mock(spec=Command)

        # cmd.run() will block until this Event is set.
        event = threading.Event()
        def wait_for_event():
            event.wait()
        cmd.run.side_effect = wait_for_event

        self.assertTrue(self.pool.isDone())

        self.pool.addCommand(cmd)
        self.assertFalse(self.pool.isDone())

        event.set()
        self.pool.join()

        self.assertTrue(self.pool.isDone())

    def test_pool_can_be_emptied_of_completed_commands(self):
        cmd = mock.Mock(spec=Command)

        self.pool.addCommand(cmd)
        self.pool.join()

        self.pool.empty_completed_items()
        self.assertEqual(self.pool.getCompletedItems(), [])

    def test_check_results_succeeds_when_no_items_fail(self):
        cmd = mock.Mock(spec=Command)

        # Command.get_results() returns a CommandResult.
        # CommandResult.wasSuccessful() should return True if the command
        # succeeds.
        result = cmd.get_results.return_value
        result.wasSuccessful.return_value = True

        self.pool.addCommand(cmd)
        self.pool.join()
        self.pool.check_results()

    def test_check_results_throws_exception_at_first_failure(self):
        cmd = mock.Mock(spec=Command)

        # Command.get_results() returns a CommandResult.
        # CommandResult.wasSuccessful() should return False to simulate a
        # failure.
        result = cmd.get_results.return_value
        result.wasSuccessful.return_value = False

        self.pool.addCommand(cmd)
        self.pool.join()

        with self.assertRaises(ExecutionError):
            self.pool.check_results()
