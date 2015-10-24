/*
 *  filerepmirror.h
 *  
 *
 *  Copyright 2009-2010, Greenplum Inc. All rights reserved.
 *
 */

#ifndef CDBFILEREPMIRROR_H
#define CDBFILEREPMIRROR_H



// -----------------------------------------------------------------------------
// RECEIVER THREAD
// -----------------------------------------------------------------------------
extern void FileRepMirror_StartReceiver(void);


// -----------------------------------------------------------------------------
// CONSUMER THREAD POOL
// -----------------------------------------------------------------------------
extern void FileRepMirror_StartConsumer(void);

#endif   /* CDBFILEREPMIRROR_H */

