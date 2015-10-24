/*-------------------------------------------------------------------------
 *
 * cdb_table.h
 *
 * Structures and functions to read the cbd tables using libpq
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDB_TABLE_H
#define CDB_TABLE_H

typedef struct cdb_basetable
{
	int			tbloid;
	char	   *pszTableName;
	char	   *pszNspName;
	bool		partitioned;
} CDBBaseTable;

/* --------------------------------------------------------------------------------------------------
 * Structure for an array of CDBBaseTables
 */

typedef struct cdb_basetable_array
{
	int			count;	  //count
	CDBBaseTable *pData;  //array of count CDBBaseTable
} CDBBaseTableArray;

/* --------------------------------------------------------------------------------------------------
 * Structure for reading gp_segment_instance
 */
typedef struct gp_segment_instance
{
	int			segid;
	bool		isValid;
	char	   *dbname;
	char	   *dbhostname;
	int			dbport;
	char	   *dbdatadir;
	char	   *dbsuperuser;
} CDBSegmentInstance;

/* --------------------------------------------------------------------------------------------------
 * Structure for an array of CDBSegmentInstances
 */
typedef struct gp_segment_instance_array
{
	int			count;	  		//count
	CDBSegmentInstance *pData;  //array of count CDBSegmentInstances
} CDBSegmentInstanceArray;


/* Free the memory allocated inside the CDBBaseTableArray.	Doesn't free the pTableAr itself. */
extern void FreeCDBBaseTableArray(CDBBaseTableArray *pTableAr);

/* Free the memory allocated inside the CDBSegmentInstanceArray.  Doesn't free the pSegmentInstanceAr itself. */
extern void FreeCDBSegmentInstanceArray(CDBSegmentInstanceArray *pSegmentInstanceAr);

/* Read all the rows in the join of cdb_basetable, pg_class, and pg_namespace */
extern bool GetCDBBaseTableArray(PGconn *pConn, CDBBaseTableArray *pTableAr);

/* Read all the rows in the gp_segment_instance table */
extern bool GetCDBSegmentInstanceArray(PGconn *pConn, const char *pszDBName, CDBSegmentInstanceArray *pSegmentInstanceAr);

#endif   /* CDB_TABLE_H */
