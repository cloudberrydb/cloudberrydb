from olap.groupingfunction import GroupingFunctionTestCase

class GroupingFunctionTests(GroupingFunctionTestCase):
    '''
    @tags SplitAgg HAWQ ORCA
    @product_version gpdb: [4.2.6.0-9.9.99.99] , hawq: [1.1.4.0-9.9.99.99]
    this test is designed to test grouping functions
    '''
    sql_dir = 'sql/'
    ans_dir = 'expected/'
    out_dir = 'output/'

