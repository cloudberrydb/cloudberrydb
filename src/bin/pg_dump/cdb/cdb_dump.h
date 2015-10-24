/*-------------------------------------------------------------------------
 *
 * cdb_dump.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDB_DUMP_H
#define CDB_DUMP_H

#include "cdb_seginst.h"


/* --------------------------------------------------------------------------------------------------
 * Structure for a cdb_dump thread parm
 */
typedef struct thread_parm
{
	pthread_t	thread;
	SegmentDatabase *pTargetSegDBData;
	SegmentDatabase *pSourceSegDBData;	/* used only for restore */
	InputOptions *pOptionsData;
	char	   *pszRemoteBackupPath;
	bool		bSuccess;
	char	   *pszErrorMsg;
}	ThreadParm;

/* --------------------------------------------------------------------------------------------------
 * Structure for an array of thread parms
 */
typedef struct thread_parm_array
{
	int			count; 	  //count
	ThreadParm *pData;	  //array of count ThreadParm
}	ThreadParmArray;

#endif   /* CDB_DUMP_H */
