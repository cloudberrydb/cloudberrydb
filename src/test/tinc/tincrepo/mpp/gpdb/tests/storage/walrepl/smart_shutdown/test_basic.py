#!/usr/bin/env python

"""
Copyright (c) 2004-Present Pivotal Software, Inc.

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

from gppylib.commands.base import Command
from gppylib.db import dbconn
from tinctest import logger
from mpp.lib.PSQL import PSQL
from mpp.models import MPPTestCase

from mpp.gpdb.tests.storage.walrepl.gpinitstandby import GpinitStandby
from mpp.gpdb.tests.storage.walrepl.lib.standby import Standby
from mpp.gpdb.tests.storage.walrepl.lib import polling

import os
import subprocess
import socket
import time
import shutil
import sys
import signal
import cStringIO
import re

class smart_shutdown(MPPTestCase):

    def setUp(self):
        # Remove standby if present
        GpinitStandby().run(option='-r')
        super(smart_shutdown, self).setUp()

    def last_ckpt_lsn(self, pg_control_dump):

        lines  = pg_control_dump.splitlines()
        ckpt_lsn = None
        split_line = []
        for line in range(0, len(lines) - 1):
            if ((lines[line]).find("Latest checkpoint location") != -1):
                split_line = re.split(r'\s+',(lines[line]).strip())
                break

        self.assertTrue(len(split_line) > 0)
        return split_line[3]

    def last_mod_file(self, path):

        self.assertTrue(os.path.exists(path))

        max_mtime = 0
        max_file = ''
        for root, dirs, files in os.walk(path):
            for fname in files:
                full_path = os.path.join(path, fname)
                mtime = os.stat(full_path).st_mtime
                if mtime > max_mtime:
                    max_mtime = mtime
                    max_file = fname
        return (os.path.join(path,max_file));

    def count_walsender(self):
        sql = ("SELECT count(*) FROM pg_stat_replication")
        with dbconn.connect(dbconn.DbURL()) as conn:
            curs = dbconn.execSQL(conn, sql)
            results = curs.fetchall()

        return results[0][0]
