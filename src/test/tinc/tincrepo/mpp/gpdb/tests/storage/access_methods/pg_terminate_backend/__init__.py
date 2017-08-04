"""
Copyright (C) 2004-2015 Pivotal Software, Inc. All rights reserved.

This program and the accompanying materials are made available under
the terms of the under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os  
import time
import tinctest
from gppylib.commands.base import Command, REMOTE
from tinctest.case import TINCTestCase
from tinctest.lib import local_path
from mpp.lib.PSQL import PSQL

class PGTerminateBackendTincTestCase(TINCTestCase):
    """
    Verifying MPP-21545, pg_terminate_backend causes 'PANIC'
    """

    PG_LOG_DIR = os.path.join(os.environ.get('MASTER_DATA_DIRECTORY'),'pg_log')
    db_name = os.environ.get('PGDATABASE')

    def __init__(self, methodName):
        super(PGTerminateBackendTincTestCase,self).__init__(methodName)
        self.terminate_backend_message = 'test pg_terminate_backend'

    def setup_table(self):
        create_table = 'create table foo as select i a, i b from generate_series(1, 10)i;'
        PSQL.run_sql_command(create_table, flags = '-q -t', dbname = self.db_name)

    def backend_terminate(self):
        """
        Get the backend process pid by issuing a query to table pg_stat_activity, then
        execute pg_terminate_backend(pid)
        """
        MAX_TRY = 5
        counter = 0

        # XXX: It's best effort to grab the procid for `create temp table t %`. It's not guaranted to be the oldest one.
        # Occasionally, with ORCA enabled, there are multiple slices reported with same statement, but different procids.
        # In this case, the oldest one is usually the first procid for the entire query.
        get_backend_pid = 'SELECT procpid FROM pg_stat_activity WHERE current_query like \'create temp table t as select%\' ORDER BY procpid LIMIT 1;'
        result = PSQL.run_sql_command(get_backend_pid, flags = '-q -t', dbname = self.db_name)
        tinctest.logger.info('result from getting backend procepid is %s'%result)
        procpid = result.strip()
        while not procpid and counter < MAX_TRY:
            result = PSQL.run_sql_command(get_backend_pid, flags = '-q -t', dbname = self.db_name)
            procpid = result.strip()
            counter += 1
        if counter == MAX_TRY:
            raise Exception('unable to select out the backend process pid')

        kill_backend = 'SELECT pg_terminate_backend(%s, \'%s\');'%(procpid, self.terminate_backend_message)
        result = PSQL.run_sql_command(kill_backend, flags = '-q -t', dbname = self.db_name)
        tinctest.logger.info('result from pg_terminate_backend is %s'%result)
        if 't' != result.strip():
            raise Exception('Failed to execute pg_terminate_backend!')

        # check if the process was terminated already
        result = PSQL.run_sql_command(get_backend_pid, flags = '-q -t', dbname = self.db_name)
        procpid_after_terminate = result.strip()
        counter = 0
        while procpid_after_terminate and counter < MAX_TRY:
            result = PSQL.run_sql_command(get_backend_pid, flags = '-q -t', dbname = self.db_name)
            procpid_after_terminate = result.strip()  
            counter += 1
            time.sleep(1)
        if counter == MAX_TRY:
            raise Exception('Running pg_terminated_backend failed!')

    def backend_start(self):
        """Run backend process """
        backend_sql = '''create temp table t as select (select case when pg_sleep(20)
                is null then 1 end from foo limit 1) from foo;'''
        results = {'rc':0, 'stdout':'', 'stderr':''}
        PSQL.run_sql_command(backend_sql, flags = '-q -t', dbname = self.db_name, results = results)
        if self.terminate_backend_message not in results['stderr']:
            raise Exception('Backend should be terminated with message \'%s\'!' % self.terminate_backend_message)

    def get_linenum_pglog(self):
        """
        Get the latest log file, and count the current max line number before starting
        pg_terminate_backend(), record the value by using tmp files. Here not able to
        use global variables, since Tinc generates many new instances according to number
        of test cases, variable value will be cleaned up.
        """
        files = [os.path.join(self.PG_LOG_DIR, fname) for fname in os.listdir(self.PG_LOG_DIR)]
        logfile = max(files, key=os.path.getmtime)
        log_dir = os.path.join(local_path(''), 'log.txt')
        linenum_dir = os.path.join(local_path(''), 'linenum.txt')
        Command('clean file','rm -rf %s'%log_dir).run()
        Command('save value','echo \'%s\'>>%s'%(logfile,log_dir)).run()
        with open(logfile) as fin:
            linenum = sum(1 for line in fin)
        Command('clean file','rm -rf %s'%linenum_dir).run()
        Command('save line number','echo \'%s\'>>%s'%(linenum,linenum_dir)).run()

    def verify_mpp21545(self):
        """
        Customer reported this issue where postmaster got PANIC while a certain backend got terminated.
        This will trigger the recovery process, and disturb the availability of the entire cluster.

        This verification is to ensure there is NO PANIC on postmaster.

        After the pg_terminate_backend(), check if new log file has been generated, if so,
        append all new content into a string, plus new content from previous log file(if log
        msg cross two logfiles).
        Check if new content contains 'PANIC', OR 'Unexpected internal error',or 'Stack trace',
        """
        files = [os.path.join(self.PG_LOG_DIR, fname) for fname in os.listdir(self.PG_LOG_DIR)]
        latest_logfile = max(files, key=os.path.getmtime)
        with open (local_path('log.txt')) as fin:
            logfile = fin.read().replace('\n', '')
        with open (local_path('linenum.txt')) as fin:
            linenum = int(fin.read().replace('\n', ''))
        new_logfile_content = ''
        previous_logfile_new_content = ''
        if latest_logfile != logfile:
            with open(latest_logfile) as fin:
                new_logfile_content = fin.read().replace('\n', '')
        with open(logfile) as fin:
            for i, line in enumerate(fin):
                if i>=linenum:
                    previous_logfile_new_content += line
                else:
                    continue
        new_log_content = previous_logfile_new_content+' '+new_logfile_content
        self.assertNotRegexpMatches(new_log_content,
            "PANIC",
            "pg_terminate_backend() should not cause PANIC !")
        self.assertNotRegexpMatches(new_log_content,
            "Unexpected internal error",
            "pg_terminate_backend() should not cause Unexpected error !")
        self.assertNotRegexpMatches(new_log_content,
            "Stack trace",
            "pg_terminate_backend() should not cause Stack trace !")
