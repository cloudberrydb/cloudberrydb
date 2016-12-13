from mpp.models import SQLTestCase

class test_static_selection(SQLTestCase):
    '''
    Includes testing for static partition selection (MPP-24709, GPSQL-2879)
    '''
    sql_dir = 'sql/'
    ans_dir = 'expected/'
    out_dir = 'output/'
