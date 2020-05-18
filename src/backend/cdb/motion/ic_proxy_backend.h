/*-------------------------------------------------------------------------
 *
 * ic_proxy_backend.h
 *
 *
 * Copyright (c) 2020-Present Pivotal Software, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef IC_PROXY_BACKEND_H
#define IC_PROXY_BACKEND_H

#include "postgres.h"

#include "cdb/cdbinterconnect.h"

extern int ic_proxy_backend_connect(ChunkTransportStateEntry *pEntry,
									MotionConn *conn);

#endif   /* IC_PROXY_BACKEND_H */
