import os
import sys
import datetime
import math
import tinctest
import re
from time import gmtime, strftime

import unittest2 as unittest
from fnmatch import fnmatch
from mpp.models import SQLTestCase

import tinctest

from tinctest.lib import run_shell_command, Gpdiff

from mpp.lib.PSQL import PSQL

@tinctest.skipLoading("Test model")
class GroupingFunctionTestCase(SQLTestCase):

    def __init__(self, methodName, baseline_result = None, sql_file = None, db_name = None):
        super(GroupingFunctionTestCase, self).__init__(methodName, baseline_result, sql_file, db_name)
        self.optimizer_mode='on'

    def _infer_metadata(self):
        super(GroupingFunctionTestCase, self)._infer_metadata()
        self.executemode= self._metadata.get('executemode', 'positivetest')

    def run_test(self):
        sql_file = self.sql_file
        ans_file = self.ans_file
        
        source_file = sys.modules[self.__class__.__module__].__file__
        source_dir = os.path.dirname(source_file)
        out_file = os.path.join(self.get_out_dir(), os.path.basename(sql_file).replace('.sql', '.out'))
        extrainfo = os.path.join(self.get_out_dir(), os.path.basename(sql_file).replace('.sql', '_extrainfo.diff'))


        self.gucs.add('optimizer=on')
        self.gucs.add('client_min_messages=\'log\'')
        self.gucs.add('optimizer_log=on')
        mod_sql_file = self._add_gucs_to_sql_file(sql_file)

        PSQL.run_sql_file(mod_sql_file, dbname = self.db_name, out_file = out_file)

        init_file_path = os.path.join(os.path.dirname(sql_file), 'init_file')
        init_files = []
        if os.path.exists(init_file_path):
            init_files.append(init_file_path)

        err = ""
        if self.executemode == 'expectedfallback':
            result_plan = self._checkforstring(out_file, 'Planner produced plan :0')
            if result_plan == False:
                err = "did not fallback properly"
        else:
            result_plan = self._checkforstring(out_file, 'Planner produced plan')
            if result_plan == True:
                err = "ORCA was not used when it's suppose to"

        if len(err) > 0:
            f = open(extrainfo, 'w')
            f.write(err)
            f.close()

        result = Gpdiff.are_files_equal(out_file, self.ans_file, match_sub = init_files)


        if len(err) < 1 and result:
            return True
        else:
            return False

    def _checkforstring(self, file, string):
        f = open(file, 'r')
        content = f.readlines()
        f.close()

        for x in content:
            x = x.strip()
            if string in x:
                return True
        return False

