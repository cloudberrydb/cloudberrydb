/*
 *  cdbfilerepmirrorack.h
 *  
 *
 *  Copyright 2009-2010 Greenplum Inc. All rights reserved.
 *
 */
#ifndef CDBFILEREPMIRRORACK_H
#define CDBFILEREPMIRRORACK_H

#include "cdb/cdbfilerep.h"

/*
 *
 */
extern int FileRepAckMirror_Ack(
					FileRepIdentifier_u		fileRepIdentifier,
					FileRepRelationType_e	fileRepRelationType,
					FileRepOperation_e		fileRepOperation,
					FileRepOperationDescription_u fileRepOperationDescription,
					FileRepAckState_e		fileRepAckState,
					uint32					messageBodyLength,
					char*					messageBody);

extern void FileRepAckMirror_StartSender(void);

#endif

