/*
 *  cdbfilerepresyncworker.h
 *
 *  Copyright 2009-2010, Greenplum Inc. All rights reserved.
 *
 */

#ifndef CDBFILEREPRESYNCWORKER_H
#define CDBFILEREPRESYNCWORKER_H

#include "c.h"
#include "storage/relfilenode.h"
#include "access/xlogdefs.h"
#include "storage/fd.h"
#include "storage/dbdirnode.h"

extern void FileRepPrimary_StartResyncWorker(void);

extern bool FileRepResyncWorker_IsResyncRequest(void);

#endif   /* CDBFILEREPRESYNCWORKER_H */
