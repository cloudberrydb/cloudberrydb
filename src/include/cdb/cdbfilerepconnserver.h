/*
 *  filerepconnserver.h
 *  
 *
 *  Copyright 2009-2010 Greenplum Inc. All rights reserved.
 *
 */

#ifndef CDBFILEREPCONNSERVER_H
#define CDBFILEREPCONNSERVER_H


/*
 * Issued by main thread of FileRep process.
 */
extern int FileRepConnServer_StartListener(
			char	*hostAddress,
				/*	*/
										   
			int		portLocal);
				/*	*/			

/*
 * Waiting (if not timeout received) to get data ready for read
 * on file descriptor.
 */
extern int FileRepConnServer_Select(void);

/*
 * Issued by main thread of FileRep process.
 */
extern int FileRepConnServer_CreateConnection(void);

/*
 * Issued by receiver thread of FileRep process.
 */
extern void FileRepConnServer_CloseConnection(void);

/*
 * Issued by Receiver thread of FileRep process.
 */
extern int FileRepConnServer_ReceiveStartupPacket(void);


/*
 * Issued by Receiver thread of FileRep process.
 */
extern int FileRepConnServer_ReceiveMessageType(
				FileRepConsumerProcIndex_e *fileRepMessageType);

/*
 * Issued by Receiver thread of FileRep process.
 */
extern int FileRepConnServer_ReceiveMessageLength(uint32 *len);

/*
 * Issued by Receiver thread of FileRep process.
 */
extern int FileRepConnServer_ReceiveMessageData(
				char		*data,
				uint32		length);

/**
 * Blocks until data is available, the socket is in error, or the function is interrupted.
 *
 * return false if the function was interrupted, true otherwise.
 */
extern bool FileRepConnServer_AwaitMessageBegin(void);

#endif /* CDBFILEREPCONNSERVER_H */
