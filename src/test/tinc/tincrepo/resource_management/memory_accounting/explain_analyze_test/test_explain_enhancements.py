from mpp.models import SQLTestCase

class ExplainAnalyzeTestCase(SQLTestCase):
    """
    @product_version gpdb:[4.3.0.0-MAIN], hawq: [1.2.1.0-]
    @db_name memory_accounting
    @gpdiff False
    @gucs explain_memory_verbosity=summary
    """
    sql_dir = 'sql/'
    out_dir = 'output/'


    def verify_out_file(self, out_file, ans_file):
        """
        Override SQLTestCase, verify explain analyze output to check that all operators report memory usage and usage is positive
        """
        f = open(out_file)
        regex = re.compile('-?\d+K bytes')

        operators = 0
        usage = 0
        for line in f:
            if "->" in line:
                operators += 1
            if "Memory:  " in line:
                usage += 1

            memory_usage = regex.findall(line)

            # Verify that Peak memory in slice statistics <= Vmem reserved
            peak_obj = re.search( r'(.*) Peak memory: (.*?)K bytes .*', line, re.M|re.I)
            vmem_obj = re.search( r'(.*) Vmem reserved: (.*?)K bytes .*', line, re.M|re.I)
            if peak_obj and vmem_obj:
                peak = peak_obj.group(2)
                vmem = vmem_obj.group(2)
                # Disabling this check. Please see MPP-23072 for details
                #self.failUnless( int(peak) <= int(vmem), 'Peak memory is greater than Vmem reserved! Peak memory: ' + peak + '; Vmem reserved: ' + vmem)

            for word in memory_usage:
                # Verify that memory usage is always positive
                self.failUnless( int(word.rstrip('K bytes')) > 0, 'Memory usage is not a postive number' )

        f.close()

        # Verify that all operators in plan report memory usage
        self.failUnless( usage == operators+1, 'Memory usage is not reported by all the operators' )

class ExplainAnalyzeTestCase_Detail(SQLTestCase):
    """
    @db_name memory_accounting
    @gucs explain_memory_verbosity=detail
    """
    sql_dir = 'sql_detail/'
    ans_dir = 'expected/'
    out_dir = 'output/'
