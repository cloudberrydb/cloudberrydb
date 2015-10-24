#include "pg_config.h"

#ifdef USE_NETBACKUP

#include "xbsa.h"
#include "nbbsa.h"

/* Dump related functions to backup data to NetBackup */
extern int initBSADumpSession(char *bsaServiceHost, char *nbbsaPolicy, char *nbbsaSchedule, char *nbbsaKeyword);
extern int createBSADumpObject(char *BackupFilePathName);
extern int sendBSADumpData(int NetBackupBlockSize);
extern int endBSADumpSession(void);

/* Restore related functions to restore data from NetBackup */
extern int initBSARestoreSession(char *bsaServiceHost);
extern int getBSARestoreObject(char *BackupFilePathName);
extern int getBSARestoreData(int NetBackupBlockSize);
extern int endBSARestoreSession(void);

/* Query related functions to query objects to NetBackup */
extern char *queryBSAObject(char *BackupFilePathName);
extern int searchBSAObject(char *NetBackupQueryObject);

/* Cleanup related function to delete objects on NetBackup server */
extern int deleteBSAObjects(char *NetBackupDeleteObject);

#endif
