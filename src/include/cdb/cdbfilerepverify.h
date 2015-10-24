/*
 *	cdbfilerepverify.h
 *	
 *
 *	Copyright 2009-2010 Greenplum Inc. All rights reserved.
 *
 */


#ifndef CDBFILEREPVERIFY_H
#define CDBFILEREPVERIFY_H

#include "cdb/cdbfilerep.h"
#include "utils/pg_crc.h"

/* locking is required during enhanced block compare 
 * ability to turn off for performance testing/debugging
 */
#define FILEREP_VERIFY_LOCKING 1 

/* NO elog/verifyLog while locks held during production use
 * may be useful for debugging however 
 */
#define DEBUG_PRINTS_WHILE_LOCKS_HELD 0

/* Useful for debugging testing - really should provide a
 * gpverify option to control */
#define DO_ENHANCED_COMPARES 1

/* Should I use the list of specific subdirectories below
 * Or just the default base directory and discover any not-excluded 
 * directories below that
 */
#define LIST_SPECIFIC_SUBDIRECTORIES 0

typedef enum FileRepVerifyDirectory_e 
{
	FileRepVerifyDirectory_Base=0,

	FileRepVerifyDirectory_Global,
	
	FileRepVerifyDirectory_Clog, 

	FileRepVerifyDirectory_DistLog, 

	FileRepVerifyDirectory_MultixactMembers,

	FileRepVerifyDirectory_MultixactOffsets,  

	FileRepVerifyDirectory_Subtrans, 

	FileRepVerifyDirectory_TwoPhase,

	/*	We want to be able to find unmatched thing in the directory above 
	 * base and galobal but if we include it here it is redundant and
	 * recursively checks all the children
	 */
#define FILEREPVERIFY_DODEFAULTDIRECTORY 0
#if FILEREPVERIFY_DODEFAULTDIRECTORY
	FileRepVerifyDirectory_DefaultDirectory,
#endif

	/* exclude pg_xlog */
#define FILEREPVERIFY_DOXLOGDIRECTORY 0
#if FILEREPVERIFY_DOXLOGDIRECTORY
	FileRepVerifyDirectory_Xlog,  
#endif

	/* If add entry, handling for that entry must be added to 
	 * FileRepVerifyDirectoryToIdentifier 
	 */

	/* the number of values in this enumeration */
	FileRepVerifyDirectory__EnumerationCount,

} FileRepVerifyDirectory_e;


#define FILEREP_RELFILENODEINFO_EMPTY -1
#define FILEREP_RELFILENODEINFO_NUMCOLS 13
#define FILEREP_INVALID_RELAM -1
#define FILEREP_INVALID_RELKIND ' '
#define FILEREP_INVALID_RELSTORAGE ' '

/* TODO! FileRepVerify_CompareInfo_s */
typedef struct FileRepRelFileNodeInfo_s 
{		
	bool mirrorIdSame;
	char primaryBaseDir[MAXPGPATH+1];
	char mirrorBaseDir[MAXPGPATH+1];
	char fromBasePath[MAXPGPATH+1];
	char filename[MAXPGPATH+1];
	char fullBaseFilePath[MAXPGPATH+1];
		
	/* LOOKS LIKE NO NEED TO SEND relFileNode over the network 
	 * - just pass around locally on primary 
	 */
	bool relFileNodeValid;
	RelFileNode	relFileNode;
	uint32 segmentFileNum;

	FileRepVerify_CompareException_e exception;
	uint64  AO_logicalEof;

} FileRepRelFileNodeInfo_s;



typedef struct FileRepVerifyDirectorySummaryInfo_s 
{
	uint32 numImmediateChildren;
	pg_crc32 childrenListHashcode;
		
} FileRepVerifyDirectorySummaryInfo_s;

typedef struct FileRepVerifyFileSummaryInfo_s 
{
	pg_crc32 fileContentsHashcode;
	off_t	 fileSize;
	int		 rowHint;
	FileRepVerify_CompareException_e exception;
	int64  AO_logicalEof;
	int	   numHeapSegments; 
	
} FileRepVerifyFileSummaryInfo_s;

typedef union FileRepVerifySummaryInfo_u 
{	
	FileRepVerifyDirectorySummaryInfo_s dirSummary;
	FileRepVerifyFileSummaryInfo_s fileSummary;
	
} FileRepVerifySummaryInfo_u;

/* TODO: These things are huge!! Can we send less that this!!
 * just send actual string lengths not the max lengths of everything
 */

/* int FileRepPrimary_VerifyAllDirectoryTreeHashes complicates things
 * because base directory is includes in the directory tree listing 
 * that we compare with - do this another way
 */
typedef struct FileRepVerifySummaryItem_s 
{
	bool isFile;

	/*	put all in one and just record number of chars in the path part */
	char baseDir[MAXPGPATH+1];
	char fromBasePath[MAXPGPATH+1];
	char filename[MAXPGPATH+1];	 

	FileRepVerifySummaryInfo_u summary;
		
} FileRepVerifySummaryItem_s;


typedef struct FileRepVerifyLogControl_s 
{
	FileRepVerifyLoggingOptions_s logOptions;
	bool filesAreOpen;
	File logFile;
	File fixFile;
	File chkpntFile;
	File externalTableFile;
	File resultsFile;
	
} FileRepVerifyLogControl_s;


typedef enum FileRepVerifyRequestState_e 
{
	FileRepVerifyRequestState_NONE=0,
	FileRepVerifyRequestState_PENDING,
	FileRepVerifyRequestState_RUNNING,

/* could continue if user asked for a resume */
	FileRepVerifyRequestState_SUSPENDED,   

/* can't continue - could be user requested or a failure */
	FileRepVerifyRequestState_ABORTED,	

/* can't continue - could be user requested or a failure */
	FileRepVerifyRequestState_FAILED,  

	FileRepVerifyRequestState_COMPLETE
	
} FileRepVerifyRequestState_e;

typedef struct FileRepVerifyRequest_RestartPoint_s 
{
	bool valid;
	bool useLogical;
  
	/* type logical or physical?
	 * union of structs - one for logical and one for physical?
	 */
	int64 logicalCheckpoint_persistentSerialNum; /* Tid	 */
  
	bool filespacesComplete;
	int64 physicalCheckpoint_persistentSerialNum;
	char	physicalCheckpoint_whereNextParent[MAXPGPATH+1];
	char	physicalCheckpoint_whereNextChild[MAXPGPATH+1];

	bool	haveWrittenIgnoreInfo;

} FileRepVerifyRequest_RestartPoint_s;

typedef struct FileRepVerify_CompareStats_s 
{
	int estimateTotalCompares;	 

	int comparesComplete;  
	/* estimateFiles, filesComplete, estimateDirs, dirsComplete */

	int totalMismatches;
	int totalFileMismatches;
	int totalDirMismatches;

	int totalMismatchesResolved; 
	int totalFileMismatchesResolved;

	int numEnhancedCompares;

	int64					totalBytesVerified;
	int64					totalBytesToVerify;	


	/* 
	 * int numMatches;
	 *
	 *	//local to FileRepPrimary_VerifyDirectory
	 *	int numFiles;
	 *	int numDirs;
	 *	int numMismatchesResolved;
	 */

} FileRepVerify_CompareStats_s;

#define MAX_FILENAME_LENGTH 512
#define MAX_LOG_LEVEL 20
#define CONTENT_NUMBER_LENGTH 100
#define MAX_DIRECTORY_LENGTH 1024
#define IGNORE_PATTERN_LENGTH 1024
#define DIR_IGNOREPATH_LENGTH 1024
#define TOKEN_LENGTH FILEREP_VERIFY_MAX_REQUEST_TOKEN_LEN 

typedef struct FileRepVerifyArguments
{
	bool fullVerify;
	bool estimateOnly;
	bool printHistory;
	bool abort;
	bool suspend;
	bool resume;
	bool reportState;
	char token[TOKEN_LENGTH+1];
	char compareFile[MAX_FILENAME_LENGTH+1];
	char logLevel[MAX_LOG_LEVEL+1];
	char contentNumber[CONTENT_NUMBER_LENGTH+1];
	char directoryTree[MAX_DIRECTORY_LENGTH+1];
	char ignorePattern[IGNORE_PATTERN_LENGTH+1];
	char dirIgnorePath[DIR_IGNOREPATH_LENGTH+1];
} FileRepVerifyArguments_s;

typedef FileRepVerifyArguments_s FileRepVerifyArguments;

typedef enum FileRepVerifyRequestType_e 
{
	FileRepVerifyRequestType_FULL=0,
	FileRepVerifyRequestType_FILE,
	FileRepVerifyRequestType_DIRECTORY

} FileRepVerifyRequestType_e;

typedef enum FileRepVerifyDiffType_e 
{
	FileRepVerifyFileMissingOnPrimary=0,
	FileRepVerifyFileMissingOnMirror,
	FileRepVerifyFileSizeMismatch,
	FileRepVerifyHashMismatch,
	FileRepVerifyBlockCompareFailed,
	FileRepVerifyMissingFileForRelation,
	FielRepVerifyMissingRelationforFile
	
}FileRepVerifyDiffType_e;
/*
 * This structure will live in shared memory so it must not use any 
 * allocation to build it!
 */
/* When change FileRepVerifyRequest_s should change CHECKPOING_VERSION_ID */
#define CHECKPOINT_VERSION_ID 1
#define MAX_AUTO_RESTARTS 3
typedef struct FileRepVerifyRequest_s 
{
	/* this gets written out as the checkpoint - make sure we 
	 * are reading in correct version 
	 */
	int checkpointVersion;
	TimestampTz lastCheckpointTime;
	int	  numAutoRestarts;
	/* TODO: make token more gthan logging? functional for requesting abort etc
	 *if so take out of logControl? 
	 */
	bool estimateOnly;
	FileRepVerifyLogControl_s logControl;
	bool allLogical;
	bool allPhysical;
	bool doHash;  
	bool skipRelFiles;	   
	/* may be irrelevant - only used for physical and now don't want to do 
	 * anymore 
	 */
	bool includeFileSizeInfo;
	bool doCheapCompareFirst;  /* only for logical? */
	bool isFailed;	
	FileRepVerifyRequestState_e state;
	/* checkpoint or restart from X */
	FileRepVerifyRequest_RestartPoint_s restartInfo;
	FileRepVerify_CompareStats_s compareStats;
	FileRepVerifyArguments_s fileRepVerifyArguments;
	FileRepVerifyRequestType_e requestType;
	char verifyItem[MAXPGPATH+1];
	TimestampTz startTime;
	TimestampTz endTime;

	//Rajesh used this - why do we need both?
	struct timeval			startVerificationTime;

	bool abortRequested;
	bool suspendRequested;
	int numIgnoreFilePatterns;
	int numIgnoreDirs;
	char **ignoreFiles;
	char **ignoreDirs;
	
} FileRepVerifyRequest_s;

typedef struct FileRepVerifyMirrorRequestParams_s 
{
	bool	valid;
	char	token[FILEREP_VERIFY_MAX_REQUEST_TOKEN_LEN+1];
	int		numIgnoreFilePatterns;
	int		numIgnoreDirs;
	char	**ignoreFiles;
	char	**ignoreDirs;
	
} FileRepVerifyMirrorRequestParams_s;


extern void FileRepPrimary_StartVerification(void);
extern void FileRepMirror_StartVerification(void);

extern int FileRepMirror_ExecuteVerificationRequest(	
													FileRepMessageHeader_s	*fileRepMessageHeader, 
													char					*fileRepMessageBody, 
													void					**response, 
													uint32					*responseSizeBytes  );

extern FileRepVerifyArguments* createNewFileRepVerifyArguments(void);

extern int processVerifyRequest(
								FileRepVerifyArguments *args, 
								char *responseMsg, 
								int responseMsgLen);

extern void FileRepVerifyShmemInit(void);

extern Size FileRepVerifyShmemSize(void);

extern char* FileRepVerify_GetCurrentToken(void);

extern float FileRepVerify_GetTotalGBsVerified(void);

extern float FileRepVerify_GetTotalGBsToVerify(void);

extern void FileRepVerify_SetTotalBlocksToVerify(int64 totalBlocksToSynchronize);

extern void FileRepVerify_AddToTotalBlocksToVerify(int64 moreBlocksToSynchronize);

extern void FileRepVerify_AddToTotalBlocksVerified(int64 moreBlocksVerified);

extern struct timeval FileRepVerify_GetEstimateVerifyCompletionTime(void);

extern char *FileRepVerify_GetVerifyRequestState(void);

struct timeval FileRepVerify_GetVerifyRequestStartTime(void);

char *FileRepVerify_GetVerifyRequestMode(void);

extern char *getCompareExceptionString(FileRepRelFileNodeInfo_s relinfo);

extern bool FileRepVerification_ProcessRequests(void);

#endif


