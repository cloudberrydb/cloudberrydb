from mpp.models import SQLTestCase
import os

class test_statistics(SQLTestCase):
    '''
    @optimizer_mode on
    JIRA related to bugs in statistics
    MPP-21064: CBucket.cpp:324: Failed assertion: this->FContains(ppointLowerNew)
    MPP-21973: Orca crashes on histogram generation query used for ANALYZE
    '''
    sql_dir = 'sql/'
    ans_dir = 'expected/'
    out_dir = 'output/'

    def get_substitutions(self):
        MYD = os.path.dirname(os.path.realpath(__file__))
        gphome = os.environ["GPHOME"]
        substitutions = { '%MYD%' : MYD , '%GPHOME%': gphome}
        return substitutions
