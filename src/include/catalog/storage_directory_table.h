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

extern void UFileAddCreatePendingEntry(Relation rel, Oid spcId, char *relativePath);
extern void UFileAddDeletePendingEntry(Relation rel, Oid spcId, char *relativePath);

extern void UFileDoDeletesActions(bool isCommit);
extern void UFileAtSubCommitSmgr(void);
extern void UFileAtSubAbortSmgr(void);
extern void DirectoryTableDropStorage(Relation rel);

#endif //STORAGE_DIRECTORY_TABLE_H
