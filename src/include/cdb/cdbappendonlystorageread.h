/*-------------------------------------------------------------------------
 *
 * cdbappendonlystorageread.h
 *
 * Copyright (c) 2007-2009, Greenplum inc
 *
 *-------------------------------------------------------------------------
 */
#ifndef CDBAPPENDONLYSTORAGEREAD_H
#define CDBAPPENDONLYSTORAGEREAD_H

#include "catalog/pg_compression.h"
#include "cdb/cdbappendonlystorage.h"
#include "cdb/cdbappendonlystoragelayer.h"
#include "cdb/cdbbufferedread.h"
#include "utils/palloc.h"
#include "storage/fd.h"


/*
 * This structure contains information about the current block.
 */
typedef struct AppendOnlyStorageReadCurrent
{
	int64					headerOffsetInFile;
			/*
			 * The file offset of the current block.
			 */

	AoHeaderKind			headerKind;
			/*
			 * The Append-Only Storage block kind.
			 */

	int32					actualHeaderLen;
			/*
			 * Actual header length that includes optional header information
			 * (e.g. firstRowNum).
			 */

	int32					overallBlockLen;
			/*
			 * The byte length of the whole block.
			 */

	int32					contentOffset;
			/*
			 * The byte offset of the content (executor data) within the block.
			 * It may be compressed.
			 */

	int32					uncompressedLen;
			/*
			 * The original content length.
			 */

	int						executorBlockKind;
			/*
			 * The executor block kind.  Defined externally by executor.
			 */

	bool					hasFirstRowNum;
			/*
			 * True if the first row number was explicity specified for this block.
			 */

	int64					firstRowNum;
			/*
			 * The first row number of this block, or INT64CONST(-1) if not specified
			 * for this block.
			 */

	int32					rowCount;
			/*
			 * The number of rows in this block.
			 */

	bool					isLarge;
			/*
			 * The block read was metadata for large content.  The actual content is in one
			 * or more following small content blocks.
			 */

	bool					isCompressed;
			/*
			 * True if the small content block has compressed content.
			 * Not applicable for the large content metadata.
			 */

	int32					compressedLen;
			/*
			 * The compressed length of the content.
			 */
} AppendOnlyStorageReadCurrent;

/*
 * This structure contains read session information.  Consider the fields
 * inside to be private.
 */
typedef struct AppendOnlyStorageRead
{
	bool	isActive;

	MemoryContext memoryContext;
			/*
			 * The memory context to use for buffers and
			 * other memory needs.
			 */

    int32         maxBufferLen;
			/*
			 * The maximum Append-Only Storage Block
			 * length including all storage headers.
			 */

	int32		largeReadLen;
			/*
			 * The large read length given to the BufferedRead module.
			 */

	char		 *relationName;
			/*
			 * Name of the relation to use in system
			 * logging and error messages.
			 */
	
	char		*title;
			/*
			 * A phrase that better describes the purpose of the this open.
			 *
			 * The caller manages the storage for this.
			 */
			 
	AppendOnlyStorageAttributes		storageAttributes;
			/*
			 * The Append-Only Storage Attributes
			 * from relation creation.
			 */

	BufferedRead	bufferedRead;
			/*
			 * The BufferedRead module's object that holds read session data.
			 */

	File	file;
			/*
			 * The handle to the current open segment file.
			 */

	int64		logicalEof;
			/*
			 * The byte length of the current segment file being read.
			 */

	int32	minimumHeaderLen;
			/*
			 * The minimum block header length based on the checkum attribute.
			 * Does not include optional header data (e.g. firstRowNum).
			 */

	char		 *segmentFileName;
			/*
			 * Name of the current segment file name to use in system
			 * logging and error messages.
			 */

	int64	bufferCount;
			/*
			 * The number of blocks read since the beginning of the segment file.
			 */

	AppendOnlyStorageReadCurrent	current;
			/*
			 * Lots of information about the current block that was read.
			 */

	/* Storage attributes */
	CompressionState *compressionState;
	int               blocksize;             /* For AO or CO uncompresed block size          */
	PGFunction       *compression_functions; /* For AO or CO compression funciton pointers.  */
			/* The array index corresponds to COMP_FUNC_*   */



} AppendOnlyStorageRead;

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

/*
 * Initialize AppendOnlyStorageRead.
 *
 * The AppendOnlyStorageRead data structure is initialized
 * once for a read session and can be used to read
 * Append-Only Storage Blocks from 1 or more segment files.
 *
 * The current file to read to is opened with the
 * AppendOnlyStorageRead_OpenFile routine.
 */
extern void AppendOnlyStorageRead_Init(
	AppendOnlyStorageRead			*storageRead,
				/* The data structure to initialize. */

	MemoryContext 					memoryContext,
				/*
				 * The memory context to use for buffers and
				 * other memory needs.  When NULL, the
				 * current memory context is used.
				 */
    int32                			maxBufferLen,
				/*
				 * The maximum Append-Only Storage Block
				 * length including all storage headers.
				 */
	char							*relationName,
				/*
				 * Name of the relation to use in system
				 * logging and error messages.
				 */

	char							*title,
				/*
				 * A phrase that better describes the purpose of the this open.
				 *
				 * The caller manages the storage for this.
				 */

	AppendOnlyStorageAttributes		*storageAttributes);
				/*
				 * The Append-Only Storage Attributes
				 * from relation creation.
				 */

/*
 * Return (read-only) pointer to relation name.
 */
extern char* AppendOnlyStorageRead_RelationName(
	AppendOnlyStorageRead			*storageRead);

/*
 * Return (read-only) pointer to relation name.
 */
extern char* AppendOnlyStorageRead_SegmentFileName(
	AppendOnlyStorageRead			*storageRead);

/*
 * Finish using the AppendOnlyStorageRead session created with ~Init.
 */
extern void AppendOnlyStorageRead_FinishSession(
	AppendOnlyStorageRead			*storageRead);
				/* The data structure to finish. */

// -----------------------------------------------------------------------------
// Open and Close
// -----------------------------------------------------------------------------

/*
 * Open the next segment file to read.
 *
 * This routine is responsible for seeking to the proper
 * read location given the logical EOF.
 */
extern void AppendOnlyStorageRead_OpenFile(
	AppendOnlyStorageRead		*storageRead,

    char				 		*filePathName,
				/* The name of the segment file to open. */

    int64                		logicalEof);
				/*
				 * The snapshot version of the EOF
				 * value to use as the read end of the segment
				 * file.
				 */

/*
 * Try opening the next segment file to read.
 *
 * This routine is responsible for seeking to the proper
 * read location given the logical EOF.
 */
extern bool AppendOnlyStorageRead_TryOpenFile(
	AppendOnlyStorageRead		*storageRead,

    char				 		*filePathName,
				/* The name of the segment file to open. */

    int64                		logicalEof);
				/*
				 * The snapshot version of the EOF
				 * value to use as the read end of the segment
				 * file.
				 */

/*
 *
 * Set a temporary read range in the current open segment file.
 *
 * The beginFileOffset must be to the beginning of an Append-Only Storage block.
 *
 * The afterFileOffset serves as the temporary EOF.  It will cause ~_GetBlockInfo to return
 * false (no more blocks) when reached.  It must be at the end of an Append-Only Storage
 * block.
 *
 * When ~_GetBlockInfo returns false (no more blocks), the temporary read range is forgotten.
 */
extern void AppendOnlyStorageRead_SetTemporaryRange(
	AppendOnlyStorageRead		*storageRead,
	int64						beginFileOffset,
	int64						afterFileOffset);

/*
 * Close the current segment file.
 *
 * No error if the current is already closed.
 */
extern void AppendOnlyStorageRead_CloseFile(
	AppendOnlyStorageRead		*storageRead);


// -----------------------------------------------------------------------------
// Reading Content
// -----------------------------------------------------------------------------

/*
 *    This section describes for reading potentially long content that can be up to 1 Gb long
 *    and and/or content may have been be bulk-compressed.
 *
 *    The AppendOnlyStorageRead_GetBlockInfo routine is used to peek at the next
 *    Append-Only Storage Block and tell the caller how to handle it.
 *
 *    If the block is small and not compressed, then it may be looked at directly in the
 *    read buffer.
 *
 *    Otherwise, the caller must provide other buffer space to either reconstruct large
 *    content and/or to decompress content into.
 */

/*
 * Get information on the next Append-Only Storage Block.
 *
 * Return true if another block was found.	Otherwise,
 * when we have reached the end of the current segment
 * file.
 */
extern bool AppendOnlyStorageRead_GetBlockInfo(
	AppendOnlyStorageRead		*storageRead,

	int32						*contentLen,
				/* The total byte length of the content. */

	int 						*executorBlockKind,
				/*
				 * The executor supplied value stored in the
				 * Append-Only Storage Block header.
				 */
	int64						*firstRowNum,
				/*
				 * When the first row number for this block
				 * was explicitly set, that value is
				 * returned here.  Otherwise, INT64CONST(-1)
				 * is returned.
				 */
	int 						*rowCount,
				/* The number of rows in the content. */

	bool						*isLarge,
				/*
				 * When true, the content was longer than the
				 * maxBufferLen (i.e. blocksize) minus
				 * Append-Only Storage Block header and had
				 * to be stored in more than one storage block.
				 */
	bool						*isCompressed);
				/*
				 * When true, the content is compressed and
				 * cannot be looked at directly in the buffer.
				 */

/*
 * Read the next Append-Only storage block in buffer.
 *
 * Return true if another block was found.  Otherwise, we have reached
 * the end of the current segment file.
 */
extern bool AppendOnlyStorageRead_ReadNextBlock(
	AppendOnlyStorageRead		*storageRead);

/*
 * Return the file offset of the current Append-Only Storage Block.
 */
extern int64 AppendOnlyStorageRead_CurrentHeaderOffsetInFile(
	AppendOnlyStorageRead		*storageRead);

/*
 * Return the compressed length of the content in the current Append-Only Storage Block.
 */
extern int64 AppendOnlyStorageRead_CurrentCompressedLen(
	AppendOnlyStorageRead		*storageRead);

/*
 * Return the overall block length of the current Append-Only Storage Block.
 */
extern int64 AppendOnlyStorageRead_OverallBlockLen(
	AppendOnlyStorageRead		*storageRead);

/*
 * Get a pointer to the small non-compressed content.
 *
 * This interface provides a pointer directly into the
 * read buffer for efficient data use.
 *
 */
extern uint8 *AppendOnlyStorageRead_GetBuffer(
	AppendOnlyStorageRead		*storageRead);

/*
 * Copy the large and/or decompressed content out.
 *
 * The contentLen parameter value must match the contentLen
 * from the AppendOnlyStorageReadGetBlockInfo call.
 */
extern void AppendOnlyStorageRead_Content(
	AppendOnlyStorageRead		*storageRead,

	uint8						*contentOut,
			/* The memory to receive the contigious content. */

	int32						contentLen);
			/* The byte length of the content. */

/*
 * Skip the current block found with ~_GetBlockInfo.
 *
 * Do not decompress the block contents.
 *
 * Call this routine instead of calling ~_GetBuffer or ~_Contents that look at contents. Useful
 * when the desired row(s) are not within the row range of the current block.
 *
 */
extern void AppendOnlyStorageRead_SkipCurrentBlock(
	AppendOnlyStorageRead		*storageRead);

extern char *AppendOnlyStorageRead_ContextStr(AppendOnlyStorageRead *storageRead);

extern int errcontext_appendonly_read_storage_block(AppendOnlyStorageRead *storageRead);

extern char *AppendOnlyStorageRead_StorageContentHeaderStr(AppendOnlyStorageRead *storageRead);

extern int errdetail_appendonly_read_storage_content_header(AppendOnlyStorageRead *storageRead);

#endif   /* APPENDONLYSTORAGELAYER_H */
