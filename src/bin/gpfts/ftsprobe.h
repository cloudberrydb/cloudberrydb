/*-------------------------------------------------------------------------
 *
 * ftsprobe.h
 *	  Interface for fault tolerance service Sender.
 *
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/bin/gpfts/ftsprobe.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef USE_INTERNAL_FTS

#ifndef GPFTS_FTSPROBE_H
#define GPFTS_FTSPROBE_H
#include "postmaster/ftsprobe.h"

typedef struct
{
	int num_pairs; /* number of primary-mirror pairs FTS wants to probe */
	fts_segment_info *perSegInfos;
	fts_config *config;
} fts_context;

extern bool FtsWalRepMessageSegments(CdbComponentDatabases *context, fts_config *context_config);

CdbComponentDatabaseInfo * getActiveMaster(fts_context *context);
CdbComponentDatabaseInfo * FtsGetPeerSegment(CdbComponentDatabases *cdbs, int content, int dbid);
CdbComponentDatabaseInfo * FtsGetPeerMaster(CdbComponentDatabases *cdbs);
CdbComponentDatabaseInfo * FtsGetPeerStandby(CdbComponentDatabases *cdbs);

#endif /* GPFTS_FTSPROBE_H */
#endif