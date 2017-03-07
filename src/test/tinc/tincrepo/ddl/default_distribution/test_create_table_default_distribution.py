from mpp.models import SQLTestCase

class test_create_table_default_distribution(SQLTestCase):
       '''
       testing CREATE TABLE with default distribution
       @author garcic12
       @created 2014-07-01 18:09:00
       @description test CREATE TABLE with default distribution
       @gpopt 1.510
       @gucs gp_create_table_random_default_distribution=on
       @tags CTAS HAWQ ORCA
       @product_version gpdb: 4.3.99.99, [4.3-], 4.3.2.0ORCA1, hawq: [1.3-]
       '''

       sql_dir = 'sql/'
       ans_dir = 'expected/'
       out_dir = 'out/'
