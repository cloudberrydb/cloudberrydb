from mpp.models import SQLTestCase
from dml.functional import DMLSQLTestCase
import os

'''
Test DML Queries for ORCA
'''
class test_dml(SQLTestCase):
    """
    @optimizer_mode on
    @tags ORCA
    @db_name dmldb
    """
    sql_dir = 'sql/'
    ans_dir = 'expected/'
    out_dir = 'output/'

    def get_substitutions(self):
        MYD = os.path.dirname(os.path.realpath(__file__))
        substitutions = { '%MYD%' : MYD }
        return substitutions

class test_dml_partition(SQLTestCase):
    """
    @optimizer_mode on
    @product_version gpdb: 4.3.99.99, gpdb: [4.3.4.0-]
    @tags ORCA MPP-21090
    @db_name dmldb
    """
    sql_dir = 'sql_partition/'
    ans_dir = 'expected_partition/'
    out_dir = 'output/'

class test_dml_hawq_partition(SQLTestCase):
    """
    @optimizer_mode on
    @product_version hawq: [1.2.1.0-]
    @tags ORCA MPP-21090
    @db_name dmldb
    """
    sql_dir = 'sql_hawq_partition/'
    ans_dir = 'expected_hawq_partition/'
    out_dir = 'output/'

class test_dml_planner(SQLTestCase):
    """
    Run the hotfix test cases(for 42x)
    @optimizer_mode off
    @product_version gpdb: [4.2.6.2-], hawq: [1.1.0.2-]
    @db_name dmldb
    """
    sql_dir = 'sql_planner'
    ans_dir = 'expected/'
    out_dir = 'output/'

