import unittest2 as unittest
from gppylib.operations.gpMigratorUtil import is_supported_version, UpgradeError
from gppylib.gpversion import GpVersion

class MigratorTests(unittest.TestCase):
    def test_is_supported_version_weird(self):
        v = GpVersion('4.4.0.0 build monkey')
        with self.assertRaisesRegexp(UpgradeError, 
                               "Greenplum Version '4.4.0.0 build monkey' is not supported for upgrade"):
            is_supported_version(v)
    def test_is_supported_version_low(self):
        v = GpVersion('main') << 2
        with self.assertRaisesRegexp(UpgradeError, 
                               "Greenplum Version '4.1.0.0 build dev' is not supported for upgrade"):
            is_supported_version(v)
    def test_is_supported_version_high(self):
        v = GpVersion('99.99')
        with self.assertRaisesRegexp(UpgradeError, 
                               "To upgrade Greenplum Version '99.99 build dev' use the upgrade tool shipped with that release"):
            is_supported_version(v)
