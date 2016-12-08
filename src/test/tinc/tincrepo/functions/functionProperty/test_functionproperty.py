from functions.functionProperty import FunctionPropertySQLTestCase

class FunctionPropertyTestCase(FunctionPropertySQLTestCase):
    """
    @db_name functionproperty
    @optimizer_mode on
    @tags ORCA
    """
    sql_dir = 'sql/'
    ans_dir = 'expected/'
    out_dir = 'output/'

