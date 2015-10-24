/*
 *  cdbfilerepdefs.h
 *  
 * Definitions needed by in header files outside of cdb.
 *
 *  Copyright 2009-2010, Greenplum Inc. All rights reserved.
 *
 */
#include "postgres.h"
#include "storage/relfilenode.h"
#include "storage/itemptr.h"
#include "access/filerepdefs.h"

char * 
MirrorDataLossTrackingState_Name(MirrorDataLossTrackingState state)
{
	switch (state) {
		case MirrorDataLossTrackingState_MirrorNotConfigured:
			return "Not Configured";
			
		case MirrorDataLossTrackingState_MirrorCurrentlyUpInResync:
			return "Currently Up (In Resync)";
			
		case MirrorDataLossTrackingState_MirrorCurrentlyUpInSync:
			return "Currently Up (In Sync)";
			
		case MirrorDataLossTrackingState_MirrorDown:
			return "Mirror Down";

		default:
			return "Unknown";
	}	
}

