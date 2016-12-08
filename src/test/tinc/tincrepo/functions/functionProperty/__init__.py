import os
import sys
import datetime
import math
import tinctest
import re
from time import gmtime, strftime, sleep

import unittest2 as unittest
from fnmatch import fnmatch
from tinctest.runner import TINCTextTestResult
from tinctest.lib import run_shell_command, Gpdiff

from mpp.models import SQLTestCase
from mpp.lib.PSQL import PSQL

class FunctionPropertySQLTestCase(SQLTestCase):

    def __init__(self, methodName, baseline_result = None, sql_file = None, db_name = None):
        super(FunctionPropertySQLTestCase, self).__init__(methodName, baseline_result, sql_file, db_name)
        self.optimizer_mode = 'on'

    def _infer_metadata(self):
        super(FunctionPropertySQLTestCase, self)._infer_metadata()
        self.executemode= self._metadata.get('executemode', 'ORCA_NORMAL')
        

    def run_test(self):
        sql_file = self.sql_file
        ans_file = self.ans_file
        
        source_file = sys.modules[self.__class__.__module__].__file__
        source_dir = os.path.dirname(source_file)
        out_file = os.path.join(self.get_out_dir(), os.path.basename(sql_file).replace('.sql', '.out'))


        guc_sql_file = self._add_gucs_to_sql_file(sql_file)
        adj_ans_file = self._add_opt_diff_too_ans_file(ans_file)
        PSQL.run_sql_file(guc_sql_file, dbname = self.db_name, out_file = out_file)

        init_files = []
        init_file_path = os.path.join(self.get_sql_dir(), 'init_file')
        if os.path.exists(init_file_path):
            init_files.append(init_file_path)


        if out_file[-2:] == '.t':
            out_file = out_file[:-2]

        if adj_ans_file is not None:
            if (self.executemode == 'normal'):
                result = Gpdiff.are_files_equal(out_file, ans_file, match_sub = init_files)
            elif (self.executemode == 'ORCA_PLANNER_DIFF'):
                out_file = os.path.join(self.get_out_dir(), os.path.basename(sql_file).replace('.sql', '.out'))
                new_ans_file = os.path.join(self.get_out_dir(), os.path.basename(ans_file).replace('.ans', '_mod.ans'))
                guc_sql_file = self._add_gucs_to_sql_file(sql_file)
                self.gucs.add('optimizer=off')
                self.gucs.add('optimizer_log=off')
                guc_off_sql_file = self._add_gucs_to_sql_file(sql_file)

                PSQL.run_sql_file(guc_off_sql_file, dbname = self.db_name, out_file = new_ans_file)
                PSQL.run_sql_file(guc_sql_file, dbname = self.db_name, out_file = out_file)

                sedcmd = "sed -i -e 's/transactionid,,,,,[[:digit:]]\+,,,,[[:digit:]]\+,[[:digit:]]\+,ExclusiveLock,t,[[:digit:]]\+,/transactionid,,,,,XXXXX,,,,XXXXX,XXXXX,ExclusiveLock,t,XXXXX,/' " +new_ans_file
                sedcmd2 = "sed -i -e 's/transactionid,,,,,[[:digit:]]\+,,,,[[:digit:]]\+,[[:digit:]]\+,ExclusiveLock,t,[[:digit:]]\+,/transactionid,,,,,XXXXX,,,,XXXXX,XXXXX,ExclusiveLock,t,XXXXX,/' " +out_file
                sedcmd3 = "sed -i -e 's/pg_aoseg_[[:digit:]]\+/pg_aoseg_XXXXX/' " +new_ans_file
                sedcmd4 = "sed -i -e 's/pg_aoseg_[[:digit:]]\+/pg_aoseg_XXXXX/' " +out_file
                run_shell_command(sedcmd, "replace dynamic values in planner output with XXXXX")
                run_shell_command(sedcmd2, "replace dynamic values in ORCA output with XXXXX")
                run_shell_command(sedcmd3, "replace dynamic values in pg_aoseg.pg_aoseg_")
                run_shell_command(sedcmd4, "replace dynamic values in pg_aoseg.pg_aoseg_")

                result = Gpdiff.are_files_equal(out_file, new_ans_file, match_sub = init_files)
            else:
                result = Gpdiff.are_files_equal(out_file, adj_ans_file, match_sub = init_files)

            if result == False:
                self.test_artifacts.append(out_file.replace('.out', '.diff'))

        return result
        
    def _add_opt_diff_too_ans_file(self,ans_file):
        """
        @param ans_file Path to the test ans file
        @returns path to the modified ans file
        """
        
        gucs_ans_file = os.path.join(self.get_out_dir(), os.path.basename(ans_file).replace('.ans', '_mod.ans'))
        start_of_plan = 0

        with open(gucs_ans_file, 'w') as o:
            with open(ans_file, 'r') as f:
                for line in f:
                    if (start_of_plan == 0):
                        if (line.find('QUERY PLAN') > 0):
                            start_of_plan = 1;
                            o.write(line)
                            continue 
                        else:
                            o.write(line)
                            continue
                    else:
                        if (line.find(' Settings:  gp_optimizer=on') > 0):
                            continue
                        elif (line.find('rows)') > 0):
                            matchrows = re.match('\(([0-9]+) rows\)', line)
                            new_row = -1 
                            if matchrows != None:
                                row_num = matchrows.group(1)
                                new_row = int(row_num) + 1
                            o.write('(' +str(new_row)+ ' rows)\n')
                            continue
                        else:
                            o.write(line)
                            continue
                                
        return gucs_ans_file


