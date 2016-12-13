#!/usr/bin/env python
import os,sys
from optparse import OptionParser
from fnmatch import fnmatch
import re
'''
'''

class GenSplitAgg(object):

    curfile = os.path.realpath(__file__)
    currentdir = os.path.dirname(curfile)
    sqlfileloc = os.path.join(currentdir, 'sql')
    ansfileloc = os.path.join(currentdir, 'expected')

    #lineitem2
    T1 = {}
    T1['tablename'] = "lineitem2"
    T1['Distcol1'] = "l_partkey"
    T1['Distcol2'] = "l_orderkey"
    T1['NonDistcol1'] = "l_suppkey"
    T1['NonDistcol2'] = "l_linenumber"

    #part2 (created by joining part and partsupp on partkeys
    T2 = {}
    T2['tablename'] = "part2"
    T2['Distcol1'] = "p_partkey"
    T2['Distcol2'] = "ps_suppkey"
    T2['NonDistcol1'] = "p_size"
    T2['NonDistcol2'] = "ps_availqty"
    



    SK_GROUP1 = [
    [ 'SK1_1' , 'SELECT %AGG1%(DISTINCT %NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY%' ],
    [ 'SK2_1' , 'SELECT %AGG1%(DISTINCT %NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL1%' ],
    [ 'SK2_2' , 'SELECT %AGG1%(DISTINCT %NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL1%, %DISTRIBUTED_COL2%' ],
    [ 'SK2_3' , 'SELECT %AGG1%(DISTINCT %NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL1% ORDER BY %DISTRIBUTED_COL1%' ],
    [ 'SK3_1' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %NONDISTRIBUTED_COL1%' ],
    [ 'SK3_2' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %NONDISTRIBUTED_COL1%, %NONDISTRIBUTED_COL2%' ],
    [ 'SK3_3' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %NONDISTRIBUTED_COL1% ORDER BY %NONDISTRIBUTED_COL1%' ],
#    [ 'SK4_1' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL2%, %NONDISTRIBUTED_COL1%' ],
#    [ 'SK4_2' , 'SELECT %AGG1%(DISTINCT %NONDISTRIBUTED_COL2%) AS %DQANUM%_dqacol1 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL2%, %NONDISTRIBUTED_COL1%' ],
    [ 'SK2_4' , 'SELECT %AGG1%(DISTINCT %NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(%NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL1%, %DISTRIBUTED_COL2%' ],
    [ 'SK2_5' , 'SELECT %AGG1%(DISTINCT %NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(%NONDISTRIBUTED_COL2%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL1%, %DISTRIBUTED_COL2%' ],
    [ 'SK2_6' , 'SELECT %AGG1%(DISTINCT %NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(DISTINCT %NONDISTRIBUTED_COL1%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL1%, %DISTRIBUTED_COL2%' ],
    [ 'SK3_4' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(%DISTRIBUTED_COL1%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %NONDISTRIBUTED_COL1%, %NONDISTRIBUTED_COL2%' ],
    [ 'SK3_5' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(%DISTRIBUTED_COL2%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %NONDISTRIBUTED_COL1%, %NONDISTRIBUTED_COL2%' ],
    [ 'SK3_6' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %NONDISTRIBUTED_COL1%, %NONDISTRIBUTED_COL2%' ]]
#    [ 'SK4_3' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(%DISTRIBUTED_COL1%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL2%, %NONDISTRIBUTED_COL1%' ],
#    [ 'SK4_4' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(%DISTRIBUTED_COL2%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL2%, %NONDISTRIBUTED_COL1%' ],
#    [ 'SK4_5' , 'SELECT %AGG1%(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol1, SUM(DISTINCT %DISTRIBUTED_COL1%) AS %DQANUM%_dqacol2 FROM %TABLE1% %SK8_OUTTER_QUERY% GROUP BY %DISTRIBUTED_COL2%, %NONDISTRIBUTED_COL1%' ]]



    SK_GROUP2_1DQA = [
    [ 'SK5_1' , 'SELECT * FROM %TABLE2%, (%DQA1%) as t where t.DQA1_dqacol1 = %TABLE2%.%T2_DISTRIBUTED_COL1%' ],
    [ 'SK6_1' , 'SELECT * FROM %TABLE2%, (%DQA1%) as t where t.DQA1_dqacol1 = %TABLE2%.%T2_NONDISTRIBUTED_COL1%' ],
    [ 'SK9_1' , '(SELECT %TABLE2%.%T2_DISTRIBUTED_COL1% FROM %TABLE2%) UNION (SELECT * FROM (%DQA1%) as t)' ],
    [ 'SK9_2' , '(SELECT * FROM (%DQA1%) as t) UNION (SELECT %TABLE2%.%T2_DISTRIBUTED_COL1% FROM %TABLE2%)' ]]

    SK_GROUP2_1DQA_DQA_MOD = [
    [ 'SK8_1', 'SELECT * from %TABLE2% WHERE %TABLE2%.%T2_DISTRIBUTED_COL1% = ANY (%DQA_MOD%) '],
    [ 'SK8_2', 'SELECT * from %TABLE2% WHERE %TABLE2%.%T2_DISTRIBUTED_COL1% = ALL (%DQA_MOD%) '],
    [ 'SK8_3', 'SELECT * from %TABLE2% WHERE %TABLE2%.%T2_NONDISTRIBUTED_COL1% = ANY (%DQA_MOD%) '],
    [ 'SK8_4', 'SELECT * from %TABLE2% WHERE %TABLE2%.%T2_NONDISTRIBUTED_COL1% = ALL (%DQA_MOD%) ']]


    SK_GRUOUP2_2DQA = [
    [ 'SK7_1' , 'SELECT * FROM (%DQA1%) as t1, (%DQA2%) as t2 WHERE t1.DQA1_dqacol1 = t2.DQA2_dqacol1' ],
    [ 'SK9_3' , '(SELECT * FROM (%DQA1%) as t) UNION (SELECT * FROM (%DQA2%) as t2)' ]]

    SK8_mod_list = [
    [ ' WHERE %TABLE1%.%DISTRIBUTED_COL1% = %TABLE2%.%T2_DISTRIBUTED_COL1% ' ],
    [ ' WHERE %TABLE1%.%DISTRIBUTED_COL1% = %TABLE2%.%T2_NONDISTRIBUTED_COL1% ' ],
    [ ' WHERE %TABLE1%.%DISTRIBUTED_COL2% = %TABLE2%.%T2_DISTRIBUTED_COL1% ' ],
    [ ' WHERE %TABLE1%.%DISTRIBUTED_COL2% = %TABLE2%.%T2_NONDISTRIBUTED_COL1% ' ],
    [ ' WHERE %TABLE1%.%NONDISTRIBUTED_COL1% = %TABLE2%.%T2_DISTRIBUTED_COL1% ' ],
    [ ' WHERE %TABLE1%.%NONDISTRIBUTED_COL1% = %TABLE2%.%T2_NONDISTRIBUTED_COL1% ' ]]

    SK8_outter_ref = []

    for x in SK8_mod_list:
        query = x[0]
        query = query.replace('%TABLE1%', T1['tablename'])
        query = query.replace('%TABLE2%', T2['tablename'])
        query = query.replace('%DISTRIBUTED_COL1%', T1['Distcol1'])
        query = query.replace('%DISTRIBUTED_COL2%', T1['Distcol2'])
        query = query.replace('%NONDISTRIBUTED_COL1%', T1['NonDistcol1'])
        query = query.replace('%NONDISTRIBUTED_COL2%', T1['NonDistcol2'])
        query = query.replace('%T2_DISTRIBUTED_COL1%', T2['Distcol1'])
        query = query.replace('%T2_DISTRIBUTED_COL2%', T2['Distcol2'])
        query = query.replace('%T2_NONDISTRIBUTED_COL1%', T2['NonDistcol1'])
        query = query.replace('%T2_NONDISTRIBUTED_COL2%', T2['NonDistcol2'])
        SK8_outter_ref.append(query)
        


    def makesql(self):
        DQA1 = []
        DQA1_mod = []
        DQA2 = []
        agg = [ 'COUNT', 'SUM' ]
        querylist = []


        #FOR T1
        for tc in self.SK_GROUP1:
            tcname_prefix = tc[0]
            tcbody = tc[1]
            for a in agg:
                thistestname = self.T1['tablename']+ '_' +tcname_prefix+ "_" +a
                query = tcbody
                query = query.replace('%AGG1%', a)
                query = query.replace('%DQANUM%', 'DQA1')
                query = query.replace('%TABLE1%', self.T1['tablename'])
                query = query.replace('%DISTRIBUTED_COL1%', self.T1['Distcol1'])
                query = query.replace('%DISTRIBUTED_COL2%', self.T1['Distcol2'])
                query = query.replace('%NONDISTRIBUTED_COL1%',  self.T1['NonDistcol1'])
                query = query.replace('%NONDISTRIBUTED_COL2%',  self.T1['NonDistcol2'])
                tmp = query.replace('%SK8_OUTTER_QUERY%', '')
                DQA1.append(tmp)
                querylist.append([ thistestname , tmp])
                for ref in self.SK8_outter_ref:
                    if 'dqacol2' not in query:
                        DQA1_mod.append(query.replace('%SK8_OUTTER_QUERY%', ref ))

            
        #FOR T2
        for tc in self.SK_GROUP1:
            tcname_prefix = tc[0]
            tcbody = tc[1]
            for a in agg:
                query = tcbody
                query = query.replace('%AGG1%', a)
                query = query.replace('%DQANUM%', 'DQA2')
                query = query.replace('%TABLE1%', self.T2['tablename'])
                query = query.replace('%DISTRIBUTED_COL1%', self.T2['Distcol1'])
                query = query.replace('%DISTRIBUTED_COL2%', self.T2['Distcol2'])
                query = query.replace('%NONDISTRIBUTED_COL1%',  self.T2['NonDistcol1'])
                query = query.replace('%NONDISTRIBUTED_COL2%',  self.T2['NonDistcol2'])
                DQA2.append(query.replace('%SK8_OUTTER_QUERY%', ''))



        #GROUP 2 - skeletons that uses the DQAs generated earlier
       
        for g2 in self.SK_GROUP2_1DQA:
            tcname_prefix = self.T1['tablename']+ '_' +g2[0]
            query = g2[1]
            query = query.replace('%TABLE2%', self.T2['tablename'])
            query = query.replace('%T2_DISTRIBUTED_COL1%', self.T2['Distcol1'])
            query = query.replace('%T2_NONDISTRIBUTED_COL1%', self.T2['NonDistcol1'])
            count = 0
            for d in DQA1:
                tcname = tcname_prefix+ '_' +str(count)
                part = d
                tmp = query.replace('%DQA1%', part)
                if 'dqacol2' not in tmp:
                    querylist.append([tcname, tmp])
                    count += 1

        for g2 in self.SK_GROUP2_1DQA_DQA_MOD:
            tcname_prefix = self.T1['tablename']+ '_' +g2[0]
            query = g2[1]
            query = query.replace('%TABLE2%', self.T2['tablename'])
            query = query.replace('%T2_DISTRIBUTED_COL1%', self.T2['Distcol1'])
            query = query.replace('%T2_NONDISTRIBUTED_COL1%', self.T2['NonDistcol1'])
            count = 0
            for d in DQA1_mod:
                tcname = tcname_prefix+ '_' +str(count)
                part = d
                tmp = query.replace('%DQA_MOD%', part)
                querylist.append([tcname, tmp])
                count += 1


        #for 2DQA queries            
        for m in self.SK_GRUOUP2_2DQA:
            tcname_prefix = self.T1['tablename']+ '_' +m[0]
            query = m[1]
            count = 0
            for d1 in DQA1:
                for d2 in DQA2:
                    tcname = tcname_prefix+ '_' + str(count)
                    part1 = d1
                    part2 = d2
                    tmp = query.replace('%DQA1%', part1)
                    tmp = tmp.replace('%DQA2%', part2)
                    if 'dqacol2' in part1:
                        if 'dqacol2' in part2:
                            querylist.append([tcname, tmp])
                            count += 1
                    else:
                        if 'dqacol2' not in part2:
                            querylist.append([tcname, tmp])
                            count += 1

        #create test files(sql and empty ans files)
        for x in querylist:
            print x[1]
            self.writefile(x[0], x[1])
                    

        
    def writefile(self, file, query):
        sqlfilename = file+ '.sql'
        ansfilename = file+ '.ans'

        sqlfile = open( os.path.join(self.sqlfileloc, sqlfilename), 'w')
        ansfile = open( os.path.join(self.ansfileloc, ansfilename), 'w')

        sqlfile.write("-- @author tungs1\n")
        sqlfile.write("-- @modified 2013-07-17 12:00:00\n")
        sqlfile.write("-- @created 2013-07-17 12:00:00\n")
        sqlfile.write("-- @description SplitDQA " + sqlfilename+ '\n')
        sqlfile.write("-- @db_name splitdqa\n")
        sqlfile.write("-- @tags SplitAgg HAWQ\n")
        sqlfile.write(query+ '\n')
        sqlfile.close()
        ansfile.close()


def main():
    x = GenSplitAgg()
    x.makesql()
        
if __name__ == "__main__": main()



