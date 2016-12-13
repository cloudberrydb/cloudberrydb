from query.aggregate.splitAgg import SplitAggSQLTestCase

class SplitAggTests(SplitAggSQLTestCase):
    """
    @db_name splitdqa
    @optimizer_mode on
    @product_version gpdb: [4.2.6.0-9.9.99.99] , hawq: [1.1.4.0-9.9.99.99]
    @tags SplitAgg HAWQ ORCA
    """
    sql_dir = 'sql/'
    ans_dir = 'expected/'
    out_dir = 'output/'

    pass

