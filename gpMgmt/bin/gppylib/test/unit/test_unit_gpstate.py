import unittest
import mock

from pygresql import pgdb

from gppylib import gparray
from gppylib.programs.clsSystemState import *

#
# Creation helpers for gparray.Segment.
#

def create_segment(**kwargs):
    """
    Takes the same arguments as gparray.Segment's constructor, but with friendly
    defaults to minimize typing.
    """
    content        = kwargs.get('content', 0)
    role           = kwargs.get('role', gparray.ROLE_PRIMARY)
    preferred_role = kwargs.get('preferred_role', role)
    dbid           = kwargs.get('dbid', 0)
    mode           = kwargs.get('mode', gparray.MODE_SYNCHRONIZED)
    status         = kwargs.get('status', gparray.STATUS_UP)
    hostname       = kwargs.get('hostname', 'localhost')
    address        = kwargs.get('address', 'localhost')
    port           = kwargs.get('port', 15432)
    datadir        = kwargs.get('datadir', '/tmp/')

    return gparray.Segment(content, preferred_role, dbid, role, mode, status,
                           hostname, address, port, datadir)

def create_primary(**kwargs):
    """Like create_segment() but with the role overridden to ROLE_PRIMARY."""
    kwargs['role'] = gparray.ROLE_PRIMARY
    return create_segment(**kwargs)

def create_mirror(**kwargs):
    """Like create_segment() but with the role overridden to ROLE_MIRROR."""
    kwargs['role'] = gparray.ROLE_MIRROR
    return create_segment(**kwargs)

class ReplicationInfoTestCase(unittest.TestCase):
    """
    A test case for GpSystemStateProgram._add_replication_info().
    """

    def setUp(self):
        """
        Every test starts with a primary, a mirror, and a GpStateData instance
        that is ready to record information for the mirror segment. Feel free to
        ignore these if they are not helpful.
        """
        self.primary = create_primary(dbid=1)
        self.mirror = create_mirror(dbid=2)

        self.data = GpStateData()
        self.data.beginSegment(self.mirror)

    # this test should be removed once we have replication slot information for
    # a primary
    def test_add_replication_info_does_nothing_for_primary(self):
        data = mock.Mock()
        GpSystemStateProgram._add_replication_info(data, self.primary, None)
        self.assertFalse(data.called)

    def test_add_replication_info_adds_unknowns_if_primary_is_down(self):
        self.primary.status = gparray.STATUS_DOWN
        GpSystemStateProgram._add_replication_info(self.data, self.mirror, self.primary)

        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_SENT_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_FLUSH_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_REPLAY_LOCATION))

    @mock.patch('gppylib.db.dbconn.connect', autospec=True)
    def test_add_replication_info_adds_unknowns_if_connection_cannot_be_made(self, mock_connect):
        # Simulate a connection failure in dbconn.connect().
        mock_connect.side_effect = pgdb.InternalError('connection failure forced by unit test')
        GpSystemStateProgram._add_replication_info(self.data, self.mirror, self.primary)

        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_SENT_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_FLUSH_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_REPLAY_LOCATION))

    @mock.patch('gppylib.db.dbconn.execSQL', autospec=True)
    @mock.patch('gppylib.db.dbconn.connect', autospec=True)
    def test_add_replication_info_adds_unknowns_if_pg_stat_replication_has_no_entries(self, mock_connect, mock_execSQL):
        # The cursor returned by dbconn.execSQL() should have a rowcount of 0.
        mock_execSQL.return_value.configure_mock(rowcount=0)

        GpSystemStateProgram._add_replication_info(self.data, self.mirror, self.primary)

        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_SENT_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_FLUSH_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_REPLAY_LOCATION))

    @mock.patch('gppylib.db.dbconn.execSQL', autospec=True)
    @mock.patch('gppylib.db.dbconn.connect', autospec=True)
    def test_add_replication_info_adds_unknowns_if_pg_stat_replication_has_too_many_entries(self, mock_connect, mock_execSQL):
        # The cursor returned by dbconn.execSQL() should have more than one row.
        mock_execSQL.return_value.configure_mock(rowcount=2)

        GpSystemStateProgram._add_replication_info(self.data, self.mirror, self.primary)

        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_SENT_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_FLUSH_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_REPLAY_LOCATION))

    @mock.patch('gppylib.db.dbconn.execSQL', autospec=True)
    @mock.patch('gppylib.db.dbconn.connect', autospec=True)
    def test_add_replication_info_populates_correctly_from_pg_stat_replication(self, mock_connect, mock_execSQL):
        cursor = mock.MagicMock()
        mock_execSQL.return_value = cursor

        # The cursor returned by dbconn.execSQL() should have exactly one row.
        cursor.rowcount = 1

        # Set up the row definition.
        sent_location   = '0/1000'
        flush_location  = '0/0800'
        flush_left      = 2048
        replay_location = '0/0000'
        replay_left     = 4096

        cursor.fetchone.return_value = (
            'streaming',
            sent_location,
            flush_location,
            flush_left,
            replay_location,
            replay_left,
        )

        GpSystemStateProgram._add_replication_info(self.data, self.mirror, self.primary)

        self.assertEqual('Streaming', self.data.getStrValue(self.mirror, VALUE__MIRROR_STATUS))
        self.assertEqual('0/1000', self.data.getStrValue(self.mirror, VALUE__REPL_SENT_LOCATION))
        self.assertEqual('0/0800 (2048 bytes left)', self.data.getStrValue(self.mirror, VALUE__REPL_FLUSH_LOCATION))
        self.assertEqual('0/0000 (4096 bytes left)', self.data.getStrValue(self.mirror, VALUE__REPL_REPLAY_LOCATION))

    @mock.patch('gppylib.db.dbconn.execSQL', autospec=True)
    @mock.patch('gppylib.db.dbconn.connect', autospec=True)
    def test_add_replication_info_omits_lag_info_if_WAL_locations_are_identical(self, mock_connect, mock_execSQL):
        cursor = mock.MagicMock()
        mock_execSQL.return_value = cursor

        # The cursor returned by dbconn.execSQL() should have exactly one row.
        cursor.rowcount = 1

        # Set up the row definition.
        sent_location   = '0/1000'
        flush_location  = '0/1000'
        flush_left      = 0
        replay_location = '0/1000'
        replay_left     = 0

        cursor.fetchone.return_value = (
            'streaming',
            sent_location,
            flush_location,
            flush_left,
            replay_location,
            replay_left,
        )

        GpSystemStateProgram._add_replication_info(self.data, self.mirror, self.primary)

        self.assertEqual('0/1000', self.data.getStrValue(self.mirror, VALUE__REPL_SENT_LOCATION))
        self.assertEqual('0/1000', self.data.getStrValue(self.mirror, VALUE__REPL_FLUSH_LOCATION))
        self.assertEqual('0/1000', self.data.getStrValue(self.mirror, VALUE__REPL_REPLAY_LOCATION))

    @mock.patch('gppylib.db.dbconn.execSQL', autospec=True)
    @mock.patch('gppylib.db.dbconn.connect', autospec=True)
    def test_add_replication_info_adds_unknowns_if_pg_stat_replication_is_incomplete(self, mock_connect, mock_execSQL):
        cursor = mock.MagicMock()
        mock_execSQL.return_value = cursor

        # The cursor returned by dbconn.execSQL() should have exactly one row.
        cursor.rowcount = 1

        # Set up the row definition.
        cursor.fetchone.return_value = (
            'starting',
            None,
            None,
            None,
            None,
            None,
        )

        GpSystemStateProgram._add_replication_info(self.data, self.mirror, self.primary)

        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_SENT_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_FLUSH_LOCATION))
        self.assertEqual('Unknown', self.data.getStrValue(self.mirror, VALUE__REPL_REPLAY_LOCATION))

    @mock.patch('gppylib.db.dbconn.execSQL', autospec=True)
    @mock.patch('gppylib.db.dbconn.connect', autospec=True)
    def test_add_replication_info_closes_connections_and_cursors(self, mock_connect, mock_execSQL):
        # The cursor returned by dbconn.execSQL() should have a rowcount of 0.
        mock_execSQL.return_value.configure_mock(rowcount=0)

        GpSystemStateProgram._add_replication_info(self.data, self.mirror, self.primary)

        assert mock_connect.return_value.close.called
        assert mock_execSQL.return_value.close.called
