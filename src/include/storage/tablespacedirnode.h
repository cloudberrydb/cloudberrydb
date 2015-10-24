/*-------------------------------------------------------------------------
 *
 * tablespacedirnode.h
 *	  Physical access information for tablespace directories.
 *
 * Copyright (c) 2009-2010, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef TABLESPACEDIRNODE_H
#define TABLESPACEDIRNODE_H

/*
 * Represents a tablespace directory within a filespace.
 */
typedef struct TablespaceDirNode
{
	Oid		filespace;

	Oid		tablespace;
} TablespaceDirNode;

#endif   /* TABLESPACEDIRNODE_H */

