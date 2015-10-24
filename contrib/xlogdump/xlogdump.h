/*-------------------------------------------------------------------------
 *
 * xlogdump.h
 *		Common header file for the xlogdump utility.
 *
 *
 *
 * Portions Copyright (c) 1996-2004, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

struct transInfo
{
	TransactionId		xid;
	uint32			tot_len;
	int			status;
	struct transInfo	*next;
};

typedef struct transInfo transInfo;
typedef struct transInfo *transInfoPtr;

static const char * const RM_names[RM_MAX_ID+1] = {
	"XLOG  ",					/* 0 */
	"XACT  ",					/* 1 */
	"SMGR  ",					/* 2 */
	"CLOG  ",					/* 3 */
	"DBASE ",					/* 4 */
	"TBSPC ",					/* 5 */
	"MXACT ",					/* 6 */
	"RM  7 ",					/* 7 */
	"RM  8 ",					/* 8 */
	"HEAP2 ",					/* 9 */
	"HEAP  ",					/* 10 */
	"BTREE ",					/* 11 */
	"HASH  ",					/* 12 */
	"GIN   ",					/* 13 */
	"GIST  ",					/* 14 */
	"SEQ   ",					/* 15 */
	"BM    ",					/* 16 */
	"DXLOG ",					/* 17 */
	"MMXLOG"					/* 18 */
};

/* Transactions status used only with -t option */
static const char * const status_names[3] = {
	"NOT COMMITED",					/* 0 */
	"COMMITED    ",					/* 1 */
	"ABORTED     "
};

/* XXX these ought to be in smgr.h, but are not */
#define XLOG_SMGR_CREATE	0x10
#define XLOG_SMGR_TRUNCATE	0x20

typedef struct xl_smgr_create
{
	RelFileNode rnode;
} xl_smgr_create;

typedef struct xl_smgr_truncate
{
	BlockNumber blkno;
	RelFileNode rnode;
} xl_smgr_truncate;

/* Maximum size of a null bitmap based on max number of attributes per tuple */
#define MaxNullBitmapLen	BITMAPLEN(MaxTupleAttributeNumber)
