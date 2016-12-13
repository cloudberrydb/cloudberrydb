import os
import sys
import datetime
import math
import tinctest
import re
from time import gmtime, strftime
import unittest2 as unittest
from fnmatch import fnmatch

import tinctest
from mpp.models import SQLTestCase
from tinctest.runner import TINCTextTestResult
from tinctest.lib import run_shell_command, Gpdiff
from mpp.lib.PSQL import PSQL

@tinctest.skipLoading("test model")
class SplitAggSQLTestCase(SQLTestCase):
    """
    @db_name splitdqa
    """
    db_name = 'splitdqa'
    sql_dir = 'sql/'

    def __init__(self, methodName, baseline_result = None, sql_file = None, db_name = None):
        super(SplitAggSQLTestCase, self).__init__(methodName, baseline_result, sql_file, db_name)
        self.optimizer_mode = 'on'

    def _infer_metadata(self):
        super(SplitAggSQLTestCase, self)._infer_metadata()
        self.executemode = self._metadata.get('executemode', 'normal')
        self.skipPlanID = self._metadata.get('skipPlanID', '').split(' ')

    def run_test(self):
        sql_file = self.sql_file
        ans_file = self.ans_file

        source_file = sys.modules[self.__class__.__module__].__file__
        source_dir = os.path.dirname(source_file)
        out_directory = os.path.join(source_dir, self.get_out_dir())

        ##create another sql_file to run record verification
        syscmd = "grep SELECT " +sql_file
        p = os.popen(syscmd, 'r')
        query = p.readline().strip()
        sql_file_verification = os.path.join(out_directory, os.path.basename(sql_file).replace('.sql', '_verification.sql'))
        out_file_verification = os.path.join(out_directory, os.path.basename(sql_file).replace('.sql', '_verification.out'))
        sql_out_file = os.path.join(out_directory, os.path.basename(sql_file).replace('*.sql', 'sql_temp.out'))
        syscmd = "rm " +sql_file_verification
        run_shell_command(syscmd, "rm any existing verification files")
        if self.executemode == 'normal':
            sqlcontent = [
            "--start_ignore",
            "\\timing on\n",
            "set optimizer=on;",
            "select enable_xform(\'CXformSplitDQA\');",
            "--end_ignore",
            query ]

            self._add_tofile(sql_file_verification, sqlcontent, 0)

            PSQL.run_sql_file(sql_file_verification, dbname = self.db_name, out_file = out_file_verification)
            return Gpdiff.are_files_equal(out_file_verification, ans_file )
        else:
            ##get the number of alternative plans from explain plan
            explain_file = os.path.join(out_directory, os.path.basename(sql_file).replace('.sql', '_explain.sql'))
            explain_file_out = os.path.join(out_directory, os.path.basename(sql_file).replace('.sql', '_explain.out'))
            sedcmd = "sed -e 's/^SELECT/EXPLAIN SELECT /' " +sql_file+ " > " +explain_file
            run_shell_command(sedcmd, "add explain to query")
            sedcmd = "sed -i -e 's/^(SELECT/EXPLAIN (SELECT /' " +explain_file
            run_shell_command(sedcmd, "add explain to query")
            cmd = "sed -i -e 's/^\-\- \@.*//' " +explain_file
            run_shell_command(cmd, "get rid of all the metadata for easier postprocessing")
            sqlcontent = [
            "set client_min_messages=\'log\';",
            "set optimizer_enumerate_plans=on;",
            "select enable_xform(\'CXformSplitDQA\');",
            "set optimizer=on;" ]
            self._add_tofile(explain_file, sqlcontent, 1)
            PSQL.run_sql_file(explain_file, dbname = self.db_name, out_file = explain_file_out)
            syscmd = "grep \'Number of plan alternatives:\' " +explain_file_out+ " |sed -e \'s/^.*\[OPT\]:.*alternatives: //\'"
            q = os.popen(syscmd, "r")
            y = q.readline()
            planid = 1
            tmp_result = []
            while planid <= int(y.strip()):
                sql_file_verification = os.path.join(out_directory, os.path.basename(sql_file).replace('.sql', '_verification_' +str(planid)+ '.sql'))
                syscmd = "rm " +sql_file_verification
                run_shell_command(syscmd, "rm any existing verification files")
                out_file_verification = os.path.join(out_directory, os.path.basename(sql_file).replace('.sql', '_verification_' +str(planid)+ '.out'))
                sqlcontent = [
                "--start_ignore",
                "\\timing on",
                "set optimizer=on;",
                "select enable_xform(\'CXformSplitDQA\');",
                "set optimizer_enumerate_plans=on;",
                "set optimizer_plan_id= " +str(planid)+ ";",
                "--end_ignore",
                query ]
                self._add_tofile(sql_file_verification, sqlcontent, 0)
                
                if str(planid) not in self.skipPlanID:   #skipping plans that're currently not working right now MPP21433
                    PSQL.run_sql_file(sql_file_verification, dbname = self.db_name, out_file = out_file_verification)
                    tmp_result.append(Gpdiff.are_files_equal(out_file_verification, ans_file ))
                planid += 1

            for r in tmp_result:
                if r == False:
                    return False
            return True


    def _add_tofile(self, filename, lines, pos):
        """
        @param filename we're adding line to
        @param line to add to the file
        """
        contents = []
        if os.path.isfile(filename):
            f = open(filename, 'r')
            contents = f.readlines()
            f.close()
        y = open(filename, 'w')
        count = len(lines) - 1
        while int(count) >= 0:
            contents.insert(int(pos), lines[count]+ '\n')
            count -= 1

        contents = "".join(contents)
        y.write(contents)
        y.close()
