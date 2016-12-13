from mpp.models import SQLTestCase

class test_lasj(SQLTestCase):
    """
    @optimizer_mode on
    @tags ORCA
    Test Left Anti Semi Join
    """
    sql_dir = 'sql/'
    ans_dir = 'expected/'
    out_dir = 'output/'
