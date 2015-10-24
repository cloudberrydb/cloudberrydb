/*
 *  cdbfilerepprimaryack.h
 *  
 *
 *  Copyright 2009-2010 Greenplum Inc. All rights reserved.
 *
 */


#ifndef CDBFILEREPPRIMARYACK_H
#define CDBFILEREPPRIMARYACK_H

#include "cdb/cdbfilerep.h"



// -----------------------------------------------------------------------------
// RECEIVER THREAD
// -----------------------------------------------------------------------------
extern void FileRepAckPrimary_StartReceiver(void);


// -----------------------------------------------------------------------------
// CONSUMER THREAD POOL
// -----------------------------------------------------------------------------

extern Size FileRepAckPrimary_ShmemSize(void);

extern void FileRepAckPrimary_ShmemInit(void);


	
extern int FileRepAckPrimary_NewHashEntry(
			   FileRepIdentifier_u	 fileRepIdentifier, 
			   FileRepOperation_e	 fileRepOperation,
			   FileRepRelationType_e fileRepRelationType);


extern bool FileRepAckPrimary_IsOperationCompleted(
				FileRepIdentifier_u	 fileRepIdentifier,
				FileRepRelationType_e fileRepRelationType);

extern int FileRepAckPrimary_RunConsumerVerification( 
													 void							**responseData, 
													 uint32							*responseDataLength, 
													 FileRepOperationDescription_u	*responseDesc);


extern XLogRecPtr FileRepAckPrimary_GetMirrorXLogEof(void);

extern int FileRepAckPrimary_GetMirrorErrno(void);

extern void FileRepAckPrimary_StartConsumer(void);


#endif


