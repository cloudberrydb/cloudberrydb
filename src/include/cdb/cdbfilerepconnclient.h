/*
 *  filerepconnclient.h
 *  
 *
 *  Copyright 2009-2010 Greenplum Inc. All rights reserved.
 *
 */
#ifndef CDBFILEREPCONNCLIENT_H
#define CDBFILEREPCONNCLIENT_H

#include "cdb/cdbfilerep.h"

/*
 * Issued by Sender thread of FileRep process.
 */
extern int FileRepConnClient_EstablishConnection(								
			char	*hostAddress,
			int		port,
			bool	reportError);

/*
 * Issued by Sender thread of FileRep process.
 */
extern void FileRepConnClient_CloseConnection(void);

/*
 * Issued by Sender thread of FileRep process.
 */
extern bool FileRepConnClient_SendMessage(
			  FileRepConsumerProcIndex_e	messageType, 
			  bool					messageSynchronous,
			  char					*message, 
			  uint32				messageLength);

#endif /* CDBFILEREPCONNCLIENT_H */


