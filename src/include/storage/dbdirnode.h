/*-------------------------------------------------------------------------
 *
 * dbdirnode.h
 *	  Physical access information for database directories.
 *
 * Copyright (c) 2009-2010, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef DBDIRNODE_H
#define DBDIRNODE_H

/*
 * Represents a database directory within a tablespace.
 *
 * NOTE: This structure is the first 2 fields of a RelFileNode, so you can safely CAST.
 */
typedef struct DbDirNode
{
	Oid		tablespace;

	Oid		database;
} DbDirNode;

#endif   /* DBDIRNODE_H */
