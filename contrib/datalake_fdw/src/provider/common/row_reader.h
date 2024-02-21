#ifndef ROW_READER_H
#define ROW_READER_H

#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "utils.h"

RowReader *createRowReader(MemoryContext mcxt,
						   TupleDesc tupleDesc,
						   bool *attrUsed,
						   gopherFS gopherFilesystem,
						   List *combinedScanTasks,
						   char *format,
						   ExternalTableMetadata *tableOptions);
bool rowReaderNext(RowReader *reader, InternalRecord *record);
void rowReaderClose(RowReader *reader);

/*
 * The following functions are migrated from datalake_extension.c in hashdata 3X.
 * see https://code.hashdata.xyz/hashdata/hashdata/-/blob/v3.x/gpcontrib/datalake_extension/src/datalake_extension.c?ref_type=heads
 */
ProtocolContext *createContext(dataLakeOptions *options);
void cleanupContext(ProtocolContext *context);
void protocolImportStart(dataLakeFdwScanState *scanstate, ProtocolContext *context, bool *attrUsed);


#endif // ROW_READER_H
