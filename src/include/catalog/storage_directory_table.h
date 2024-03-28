/*-------------------------------------------------------------------------
 *
 * Storage manipulation for directory table.
 *
 * Copyright (c) 2016-Present Hashdata, Inc.
 *
 * src/include/catalog/storage_directory_table.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STORAGE_DIRECTORY_TABLE_H
#define STORAGE_DIRECTORY_TABLE_H

#include "utils/relcache.h"

extern void FileAddCreatePendingEntry(Relation rel, Oid spcId, char *relativePath);
extern void FileAddDeletePendingEntry(Relation rel, Oid spcId, char *relativePath);

extern void FileDoDeletesActions(bool isCommit);
extern void FileAtSubCommitSmgr(void);
extern void FileAtSubAbortSmgr(void);
extern void DirectoryTableDropStorage(Relation rel);

#endif //STORAGE_DIRECTORY_TABLE_H
