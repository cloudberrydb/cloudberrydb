/*-------------------------------------------------------------------------
 *
 * combocid.h
 *	  Combo command ID support routines
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/combocid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMBOCID_H
#define COMBOCID_H

/*
 * HeapTupleHeaderGetCmin and HeapTupleHeaderGetCmax function prototypes
 * are in access/htup.h, because that's where the macro definitions that
 * those functions replaced used to be.
 */

extern void AtEOXact_ComboCid(void);
extern void AtEOXact_ComboCid_Dsm_Detach(void);
extern void RestoreComboCIDState(char *comboCIDstate);
extern void SerializeComboCIDState(Size maxsize, char *start_address);
extern Size EstimateComboCIDStateSpace(void);

/* Key and entry structures for the hash table */
typedef struct
{
	CommandId	cmin;
	CommandId	cmax;
} ComboCidKeyData;

typedef ComboCidKeyData *ComboCidKey;

/* Hook for combocid */
typedef void (*combocid_hook_type) ();
extern PGDLLIMPORT combocid_hook_type combocid_hook;

#endif							/* COMBOCID_H */
