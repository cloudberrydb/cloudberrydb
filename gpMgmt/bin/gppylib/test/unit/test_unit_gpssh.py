import imp
import os
import io

import sys
from mock import patch

from gp_unittest import GpTestCase


class GpSshTestCase(GpTestCase):

    def setUp(self):
        # because gpssh does not have a .py extension, we have to use imp to import it
        # if we had a gpssh.py, this is equivalent to:
        #   import gpssh
        #   self.subject = gpssh
        gpssh_file = os.path.abspath(os.path.dirname(__file__) + "/../../../gpssh")
        self.subject = imp.load_source('gpssh', gpssh_file)

    @patch('sys.exit')
    def test_when_run_without_args_prints_help_text(self, mock):
        mock.side_effect = Exception("on purpose")
        # GOOD_MOCK_EXAMPLE of stdout
        with patch('sys.stdout', new=io.BytesIO()) as mock_stdout:
            with self.assertRaisesRegexp(Exception, "on purpose"):
                self.subject.main()
        # assert on what main() was expected to do
        # in this case, check what was written to sys.stdout?
        self.assertIn('gpssh -- ssh access to multiple hosts at once', mock_stdout.getvalue())

    def test_when_remote_host_cpu_load_causes_bad_prompts_will_retry_and_succeed(self):
        pass
        # sys.argv = ['', '-h', 'localhost', 'uptime']
        # self.subject.main()
        # assert on what main() was expected to do
        # in this case, check what was written to sys.stdout?

    def test_when_remote_host_cpu_load_causes_bad_prompts_will_retry_and_fail(self):
        pass
        
