/*-------------------------------------------------------------------------
 *
 * bufferedtest.c
 *		Test the BufferedAppend (cdbbufferedappend.c and .h) and
 *      BufferedRead (cdbbufferedread.c and .h) modules.
 *
 * Usage: bufferedtest [options] < input >output
 *
 *
 * $PostgreSQL$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <fcntl.h>
#include <unistd.h>
#include <getopt_long.h>

#include "cdb/cdbbufferedappend.h"
#include "cdb/cdbbufferedread.h"
#include "cdb/cdbvarblock.h"
#include "storage/lzjb.h"		// compression routines
#include "access/htup.h"
#include "access/tupmacs.h"

#include "bufferedtest.h"

#ifdef HAVE_LIBZ
#include <zlib.h>
#endif

// static int		binaryFd;		/* binary FD for current file */

/* command-line parameters */
static bool 		append = false; /* when true we are doing append */
static bool 		read_flag = false; /* when true we are doing append */
static bool 		gdb_flag = false; /* when true we are pausing for gdb */
static bool 		word_flag = false; /* when true we are using word-lengths */
static int			blocksize = 6000;
static int			largewrite = 1 * 1024 * 1024;
static bool 		detail = false; /* when true display a lot of detail */
static bool 		summary = false; /* when true display a summary */
static bool 		varblock = false; /* when true pack lines into VarBlocks */
static bool 		one_flag = false; /* when true use one-at-time style when making/reading VarBlocks */
static bool 		compress_flag = false; /* when true use compression */
static char         *compress_flavor_str = NULL;  /* argument for the -c or --compress */
static bool 		length_flag = false; /* when true use 32-bit lengths for non-block-compressed VarBlocks */
typedef enum CompressFlavor
{
	NO_COMPRESSION,
	LZJB,
	ZLIB,
} CompressFlavor;
static CompressFlavor compress_flavor = NO_COMPRESSION;
static bool 		heaptuple_flag = false; /* when true make heaptuples (one column text table) */
static bool 		minimaltuple_flag = false; /* when true make minimaltuples (one column text table) */

#define MAX_LINEBUFFER_LEN 6000 
static uint8		lineBuffer[MAX_LINEBUFFER_LEN+1];
static bool			endOfInput = false;
static int          lineLength = 0;

static uint8		tempLineBuffer[MAX_LINEBUFFER_LEN+1];

#define MAX_VARBLOCK_BUFFER_LEN 50000
static uint8        varBlockBuffer[MAX_VARBLOCK_BUFFER_LEN];

#define MAX_VARBLOCK_TEMPSPACE_LEN 4098 
static uint8 tempSpace [MAX_VARBLOCK_TEMPSPACE_LEN];

#define MAX_HEAPTUPLE_LEN (MAX_LINEBUFFER_LEN+100)
static uint8		tempHeapTuple[MAX_HEAPTUPLE_LEN];

static void
doReadLine(void)
{
	int c = '*';
	int n = 0;

	while (true)
	{
		c = getc(stdin);
		if (c == EOF)
		{
			endOfInput = true;
			break;
		}
		if (c == '\n')
			break;
		if (n >= MAX_LINEBUFFER_LEN)
		{
			fprintf(stderr, "doReadLine: Line too long %d\n",n);
			exit(1);
		}
		lineBuffer[n] = c;
		n++;
	}
	lineBuffer[n] = '\0';
	lineLength = n;
}

static void
printAppendHeader(FILE* where, BufferedAppend  *bufferedAppend)
{
	fprintf(where,"BufferedAppend:\n");
	fprintf(where,"    memory: (%p)\n", bufferedAppend->memory);
	fprintf(where,"    memoryLen: %d\n", bufferedAppend->memoryLen);
	fprintf(where,"    maxBufferLen: %d\n", bufferedAppend->maxBufferLen);
	fprintf(where,"    largeWriteLen: %d\n", bufferedAppend->largeWriteLen);
	fprintf(where,"    largeWriteMemory: (%p)\n", bufferedAppend->largeWriteMemory);
	fprintf(where,"    afterBufferMemory: (%p)\n", bufferedAppend->afterBufferMemory);
	
	fprintf(where,"  Buffer level members:\n");
	fprintf(where,"    bufferLen: %d\n", bufferedAppend->bufferLen);
	
	fprintf(where,"  File level members:\n");
	fprintf(where,"    file: %d\n", bufferedAppend->file);
	fprintf(where,"    fileLen: %lld\n", bufferedAppend->fileLen);
}

static void
doVarBlockAppend(
	BufferedAppend  *bufferedAppend,
	uint8			*buffer,
	int32			bufferLen)
{	
	if (!length_flag)
	{
		BufferedAppendFinishBuffer(
					bufferedAppend, 
					bufferLen);
	}
	else
		{
		int32 *lengthWord;
		int32 roundUpLen;
		
	    lengthWord = (int32*)buffer;
		*lengthWord = bufferLen;

		roundUpLen = sizeof(int32) + ((bufferLen + sizeof(int32) - 1)/sizeof(int32))*sizeof(int32);
		BufferedAppendFinishBuffer(
					bufferedAppend, 
					roundUpLen);
	}
}

// Useful comment on varlena headers!
/*
 * Bit patterns used to indicate sort varlena headers:
 *
 * 00xxxxxx	4-byte length word, aligned, uncompressed data (up to 1G)
 * 01xxxxxx	4-byte length word, aligned, *compressed* data (up to 1G)
 * 10000000	1-byte length word, unaligned, TOAST pointer
 * 1xxxxxxx	1-byte length word, unaligned, uncompressed data (up to 127b)
 *
 * ...
 */
 
static void
doCopyOutOneColumnTextFromMinimalTuple(
	uint8 		*minTuplePtr,
	int32		minTupleLen,
	uint8		*line,
	int32		maxLineLen,
	int32		*lineLen)
{
//	MinimalTuple minTuple = (MinimalTuple)minTuplePtr;
	uint8 *data;
	int len;
	int hoff;

	len = offsetof(MinimalTupleData, t_bits);

//  NO NULL
//	if (hasnull)
//		len += BITMAPLEN(numberOfAttributes);

//  NO OID
//	if (tupleDescriptor->tdhasoid)
//		len += sizeof(Oid);

	hoff = len = MAXALIGN(len); /* align user data safely */

	data = (uint8*)(minTuplePtr + hoff);

	// UNDONE: Are empty varlena represented by 4-byte length?

	if ((data[0] & 0x80) != 0)	// short varlena (for us -- since we don't toast, etc.)
	{
		*lineLen = data[0] & 0x7F;
		if (*lineLen == 0)
		{
			fprintf(stderr,"Not expecting 0 length (data[0] %d, data[1] %d, data[2] %d, data[3] %d)\n",
			        data[0],
			        data[1],
			        data[2],
			        data[3]);
			exit(1);
		}
		Assert(*lineLen > 0);
		Assert(*lineLen <= 127);
		memcpy(line,data + 1,*lineLen);
	}
	else
	{
		uint32 *lengthPtr;
		
		// UNDONE: Assume properly aligned
		lengthPtr = (uint32*)data;
		*lineLen = ntohl(*lengthPtr);
		if (*lineLen > maxLineLen)
		{
			fprintf(stderr,"Line too long 0x%08x bytes more than 0x%x bytes (data[0] %d, data[1] %d, data[2] %d, data[3] %d)",
			        *lineLen, maxLineLen,
			        data[0],
			        data[1],
			        data[2],
			        data[3]);
			exit(1);
		}
		Assert(*lineLen <= maxLineLen);
		if (*lineLen > 0)
			memcpy(line,data + sizeof(int32),*lineLen);
	}
	
}

static void
doCopyOutOneColumnTextFromHeapTuple(
	uint8 		*heapTuplePtr,
	int32		heapTupleLen,
	uint8		*line,
	int32		maxLineLen,
	int32		*lineLen)
{
	Assert(heapTupleLen > MINIMAL_TUPLE_OFFSET);
	
	doCopyOutOneColumnTextFromMinimalTuple(
		heapTuplePtr + MINIMAL_TUPLE_OFFSET,
		heapTupleLen - MINIMAL_TUPLE_OFFSET,
		line,
		maxLineLen,
		lineLen);
}
static Size
doOneTextColumnFill(
	HeapTupleHeader 	td,
	uint8 				*data,
	uint8 				*source,
	int32 				sourceLen)
{
	uint8* start = data;
	
	/* varlena */
	td->t_infomask |= HEAP_HASVARWIDTH;
	
	if (sourceLen > 0 && sourceLen <= 127) 
	{
		/* no alignment for short varlenas */
		data[0] = sourceLen;
		data[0] |= 0x80;		// Turn on high bit to indicate small varlena.
		memcpy(data + 1, source, sourceLen);
		fprintf(stderr,"short varlena 0x%08x byte long data\n", data[0]);
		return sourceLen + 1;
	}
	else
	{
		/* must store full 4-byte header varlena */
		// UNDONE: Assume properly aligned
		uint32 converted = htonl(sourceLen);
		memcpy(data,&converted,sizeof(int32));
		memcpy(data + sizeof(int32), source, sourceLen);
		fprintf(stderr,"long varlena 0x%08x byte long data\n", sourceLen);

		return (data - start) + sizeof(int32) + sourceLen;
	}
}

static void
doMakeOneTextColumnHeapTuple(
	uint8 		*source,
	int32 		sourceLen,
	uint8 		*dest,
	int32 		maxDestLen,
	int32 		*destLen)
{
//	HeapTuple	tuple;			/* return tuple */
	HeapTupleHeader td;			/* tuple data */
	unsigned long len, predicted_len, actual_len;
	int			hoff;
	
	/*
	 * For now, crudely borrow from heap_formtuple, etc, so we don't have to try and bind a bunch of objects...
	 */
	 
	/*
	 * Determine total space needed
	 */
	len = offsetof(HeapTupleHeaderData, t_bits);

//  NO NULL
//	if (hasnull)
//		len += BITMAPLEN(numberOfAttributes);

//  NO OID
//	if (tupleDescriptor->tdhasoid)
//		len += sizeof(Oid);

	hoff = len = MAXALIGN(len); /* align user data safely */

//	predicted_len = ComputeDataSize(tupleDescriptor, values, nulls);
	predicted_len = 0;
	{
		// What ComputeDataSize does for this case.
		Size		data_length = 0;

		// Alignment of string data is 1.
//		data_length = att_align(data_length, att[i]->attalign);

		// Length is string length.
//		data_length = att_addlength(data_length, att[i]->attlen, values[i]);
		if (sourceLen > 0 && sourceLen <= 127) 
		{
			/* no alignment for short varlenas */
			data_length = sourceLen + 1;
		}
		else
		{
			/* must store full 4-byte header varlena */
			
			// UNDONE: Alignment???

			data_length = sizeof(int32) + sourceLen;
		}

		predicted_len = data_length;
	}
		
	len += predicted_len;
	Assert(len <= maxDestLen);

	/*
	 * Allocate and zero the space needed.	Note that the tuple body and
	 * HeapTupleData management structure are allocated in one chunk.
	 */
//	tuple = (HeapTuple) palloc0(HEAPTUPLESIZE + len);
//	tuple->t_data = td = (HeapTupleHeader) ((char *) tuple + HEAPTUPLESIZE);
	td = (HeapTupleHeader) dest;
	memset(td, 0, len);

	/*
	 * And fill in the information.  Note we fill the Datum fields even though
	 * this tuple may never become a Datum.
	 */
//	tuple->t_len = len;
//	ItemPointerSetInvalid(&(tuple->t_self));
//	tuple->t_tableOid = InvalidOid;

	HeapTupleHeaderSetDatumLength(td, len);

//  UNDONE: Do we need to pass one or both of these in as parameters to
//  UNDONE: the bufferedtest program???
//	HeapTupleHeaderSetTypeId(td, tupleDescriptor->tdtypeid);
//	HeapTupleHeaderSetTypMod(td, tupleDescriptor->tdtypmod);

	HeapTupleHeaderSetNatts(td, 1);		// One text column.
	td->t_hoff = hoff;

//	if (tupleDescriptor->tdhasoid)		/* else leave infomask = 0 */
//		td->t_infomask = HEAP_HASOID;

//	actual_len = 
//		DataFill((char *) td + hoff,
//				 tupleDescriptor,
//				 values,
//				 nulls,
//				 &td->t_infomask,
//				 (hasnull ? td->t_bits : NULL));
	actual_len =
		doOneTextColumnFill(td,
							(uint8 *) td + hoff,
							source,
							sourceLen);
	Assert(actual_len == predicted_len);

	*destLen = len;
}

static void
doCompressAppend(
	BufferedAppend  *bufferedAppend,
	uint8			*sourceData,
	int32			sourceLen)
{
	uint8 *buffer;
	size_t compressedSize;
	int32 compressedDataLen;
	int32 *lengthWord;
	int32 roundUpLen;
	
	buffer = BufferedAppendGetMaxBuffer(bufferedAppend);

	if (compress_flavor == ZLIB)
	{
#ifndef HAVE_LIBZ
							fprintf(stderr,"Don't have ZLIB on the system!");
							exit(1);
#else
		int level = 3;
		
		compressedSize = bufferedAppend->maxBufferLen;	// Input to compress2.	UNDONE: Create function to get this value.
		
		if (compress2(buffer + sizeof(int32), &compressedSize, (Bytef *)sourceData, sourceLen, level) != Z_OK)
		{
			fprintf(stderr,"Compression failed");
			exit(1);
		}
#endif
	}
	else
	{
		compressedSize = (int32)lzjb_compress(sourceData, buffer + sizeof(int32), (size_t)sourceLen);
	}
	Assert(compressedSize > 0);
	fprintf(stderr, "Compressed size %d\n",(int)compressedSize);
	compressedDataLen = (int32)compressedSize;
    lengthWord = (int32*)buffer;
	*lengthWord = compressedDataLen;


	roundUpLen = sizeof(int32) + ((compressedDataLen + sizeof(int32) - 1)/sizeof(int32))*sizeof(int32);
	fprintf(stderr,"Compressed VarBlock length %d to %d (round-up length %d)\n", sourceLen, compressedDataLen, roundUpLen);
	BufferedAppendFinishBuffer(
				bufferedAppend, 
				roundUpLen);
}

static void
doAppendVarBlock(File appendFile)
{
	int32 maxBufferLen;
	int32 memoryLen;
	uint8 *memory;
	int32 actualMaxBufferLen;
    BufferedAppend  bufferedAppend;
	uint8 *buffer;
	int32 bufferLen;
	int64 fileLen;
    VarBlockMaker  myVarBlockMaker;
	int itemCount;
    uint8 *itemPtr;
	int actualItemCount;
	int32 heapTupleLen;
	int32 minimalTupleLen;

	maxBufferLen = MAX_VARBLOCK_BUFFER_LEN;
	if (compress_flavor == LZJB || compress_flavor == ZLIB || length_flag)
	{
		actualMaxBufferLen = maxBufferLen + sizeof(int32);
		actualMaxBufferLen = ((actualMaxBufferLen + sizeof(int32) - 1)/sizeof(int32))*sizeof(int32);
	}
	else
		actualMaxBufferLen = maxBufferLen;
	
	memoryLen = BufferedAppendMemoryLen(actualMaxBufferLen, largewrite);
	memory = (uint8*)malloc(memoryLen);
	if (memory == NULL)
	{
		fprintf(stderr, "Cannot allocate memory %d bytes", memoryLen);
		exit(1);
	}

	BufferedAppendInit(
		&bufferedAppend,
		memory,
		memoryLen,
		actualMaxBufferLen,
		largewrite);

	BufferedAppendSetFile(
		&bufferedAppend,
		appendFile,
		0);

	if (compress_flavor == NO_COMPRESSION)
	{
		buffer = BufferedAppendGetMaxBuffer(&bufferedAppend);
		
	    VarBlockMakerInit(
	        &myVarBlockMaker,
	        buffer + (length_flag ? sizeof(int32) : 0),
	        maxBufferLen,
	        tempSpace,
	        MAX_VARBLOCK_TEMPSPACE_LEN);
	}
	else if (compress_flavor == LZJB || compress_flavor == ZLIB)
	{
		/*
		 * Block oriented compression.
		 */
	    VarBlockMakerInit(
	        &myVarBlockMaker,
	        varBlockBuffer,
	        maxBufferLen,
	        tempSpace,
	        MAX_VARBLOCK_TEMPSPACE_LEN);
	}
	else
	{
		fprintf(stderr, "Compression flavor '%s' not supported here", compress_flavor_str);
		exit(1);
	}
	
	itemCount = 0;
	
	while (true)
	{
		if (detail)
		{
			// UNDONE: Temporarily to stderr instead of stdout...
			printAppendHeader(stderr, &bufferedAppend);
		}
	
		doReadLine();
		if (!endOfInput || lineLength > 0)
		{
			if (heaptuple_flag || minimaltuple_flag)
			{
				int32 debugLineLen;
				
				doMakeOneTextColumnHeapTuple(
					lineBuffer,
					lineLength,
					tempHeapTuple,
					MAX_HEAPTUPLE_LEN,
					&heapTupleLen);

				fprintf(stderr,"heapTupleLen %d\n", heapTupleLen);
				doCopyOutOneColumnTextFromHeapTuple(
					tempHeapTuple,
					heapTupleLen,
					tempLineBuffer,
					MAX_LINEBUFFER_LEN,
					&debugLineLen);
				tempLineBuffer[debugLineLen] = '\0';
				fprintf(stderr,"Debug: %s\n",tempLineBuffer);

				/* Do minimal tuple adjustment */
				if (minimaltuple_flag)
				{
					minimalTupleLen = heapTupleLen - MINIMAL_TUPLE_OFFSET;
					itemPtr = VarBlockMakerGetNextItemPtr(&myVarBlockMaker,minimalTupleLen);
				}
				else
					itemPtr = VarBlockMakerGetNextItemPtr(&myVarBlockMaker,heapTupleLen);
			}
			else
				itemPtr = VarBlockMakerGetNextItemPtr(&myVarBlockMaker,lineLength);
			if (itemPtr == NULL)
			{
				actualItemCount = VarBlockMakerItemCount(&myVarBlockMaker);
				Assert(actualItemCount == itemCount);
				if (itemCount == 0)
				{
					fprintf(stderr, "Item too long (#1)\n");
					exit(1);
				}
				bufferLen = VarBlockMakerFinish(&myVarBlockMaker);	
				if (detail)
				{
					fprintf(stdout, 
						    "Finished VarBlock (length = %d, item count = %d)\n",
						    bufferLen, itemCount);
				}

				if (compress_flavor == NO_COMPRESSION)
				{
					doVarBlockAppend(&bufferedAppend,buffer,bufferLen);
					
					buffer = BufferedAppendGetMaxBuffer(&bufferedAppend);
					
				    VarBlockMakerInit(
				        &myVarBlockMaker,
				        buffer + (length_flag ? sizeof(int32) : 0),
				        maxBufferLen,
				        tempSpace,
				        MAX_VARBLOCK_TEMPSPACE_LEN);
				}
				else if (compress_flavor == LZJB || compress_flavor == ZLIB)
				{
					doCompressAppend(&bufferedAppend, varBlockBuffer, bufferLen);

				    VarBlockMakerInit(
				        &myVarBlockMaker,
				        varBlockBuffer,
				        maxBufferLen,
				        tempSpace,
				        MAX_VARBLOCK_TEMPSPACE_LEN);

				}
				else
				{
					fprintf(stderr, "Compression flavor '%s' not supported here", compress_flavor_str);
					exit(1);
				}
			
				itemCount = 0;
				itemPtr = VarBlockMakerGetNextItemPtr(&myVarBlockMaker,lineLength);
				if (itemPtr == NULL)
				{
					fprintf(stderr, "Item too long (#2)\n");
					exit(1);
				}
			}

			itemCount++;

			Assert(itemPtr != NULL);
			if (detail)
			{
				fprintf(stdout, 
					    "Copy into pointer %p\n",
					    itemPtr);
			}
			
			if (heaptuple_flag)
			{
//				int32 lineLen;
				
				memcpy(itemPtr,tempHeapTuple,heapTupleLen);
				
//				doCopyOutOneColumnTextFromHeapTuple(
//					itemPtr,
//					heapTupleLen,
//					tempLineBuffer,
//					MAX_LINEBUFFER_LEN,
//					&lineLen);
//				tempLineBuffer[lineLen] = '\0';
//				fprintf(stderr,"Debug: %s\n",tempLineBuffer);
			}
			else if (minimaltuple_flag)
			{
				memcpy(itemPtr,tempHeapTuple + MINIMAL_TUPLE_OFFSET,minimalTupleLen);
			}
			else
			{
				if (lineLength > 0)
					memcpy(itemPtr,lineBuffer,lineLength);
			}
		}

		if (endOfInput)
			break;
	}

	actualItemCount = VarBlockMakerItemCount(&myVarBlockMaker);
	Assert(actualItemCount == itemCount);
	if (itemCount > 0)
	{
		bufferLen = VarBlockMakerFinish(&myVarBlockMaker);	
		if (detail)
		{
			fprintf(stdout, 
				    "Finished final VarBlock (length = %d, item count = %d)\n",
				    bufferLen, itemCount);
		}
		if (compress_flavor == NO_COMPRESSION)
		{
			doVarBlockAppend(&bufferedAppend,buffer,bufferLen);
		}
		else if (compress_flavor == LZJB || compress_flavor == ZLIB)
		{
			doCompressAppend(&bufferedAppend, varBlockBuffer, bufferLen);
		}
		else
		{
			fprintf(stderr, "Compression flavor '%s' not supported here", compress_flavor_str);
			exit(1);
		}
	}
	
	fileLen = BufferedAppendFileLen(&bufferedAppend);
	if (detail || summary)
	{
		fprintf(stderr, "FileLen %lld\n", fileLen);
	}

	BufferedAppendCompleteFile(&bufferedAppend);

	BufferedAppendFinish(&bufferedAppend);
}

typedef struct AppendVarBlockOneContext
{
	int32				 maxBufferLen;
	uint8				 *memory;
	int32				 memoryLen;
	BufferedAppend       bufferedAppend;
	uint8                *buffer;
	VarBlockMaker        varBlockMaker;
	int					 itemCount;
} AppendVarBlockOneContext;

static AppendVarBlockOneContext OurAppendVarBlockOne;

static void
doCreateAppendVarBlockOneContext(File appendFile)
{
	OurAppendVarBlockOne.maxBufferLen = MAX_VARBLOCK_BUFFER_LEN;
	
	OurAppendVarBlockOne.memoryLen = 
					BufferedAppendMemoryLen(
								OurAppendVarBlockOne.maxBufferLen, 
								largewrite);
	OurAppendVarBlockOne.memory = 
					(uint8*)malloc(OurAppendVarBlockOne.memoryLen);
	if (OurAppendVarBlockOne.memory == NULL)
	{
		fprintf(stderr, "Cannot allocate memory %d bytes", 
				OurAppendVarBlockOne.memoryLen);
		exit(1);
	}

	BufferedAppendInit(
		&OurAppendVarBlockOne.bufferedAppend,
		OurAppendVarBlockOne.memory,
		OurAppendVarBlockOne.memoryLen,
		OurAppendVarBlockOne.maxBufferLen,
		largewrite);

	BufferedAppendSetFile(
		&OurAppendVarBlockOne.bufferedAppend,
		appendFile,
		0);
			
	OurAppendVarBlockOne.buffer = 
					BufferedAppendGetMaxBuffer(
								&OurAppendVarBlockOne.bufferedAppend);
	
    VarBlockMakerInit(
        &OurAppendVarBlockOne.varBlockMaker,
        OurAppendVarBlockOne.buffer,
        OurAppendVarBlockOne.maxBufferLen,
        tempSpace,
        MAX_VARBLOCK_TEMPSPACE_LEN);
	
	OurAppendVarBlockOne.itemCount = 0;
}

static void
doAppendVarBlockOneLine(uint8 *line, int32 length)
{
    uint8 *itemPtr;
	int actualItemCount;
	int32 bufferLen;
	
	if (detail)
	{
		// UNDONE: Temporarily to stderr instead of stdout...
		printAppendHeader(stderr, &OurAppendVarBlockOne.bufferedAppend);
	}
	
	itemPtr = VarBlockMakerGetNextItemPtr(
								&OurAppendVarBlockOne.varBlockMaker,
								length);
	if (itemPtr == NULL)
	{
		actualItemCount = VarBlockMakerItemCount(
								&OurAppendVarBlockOne.varBlockMaker);
		Assert(actualItemCount == OurAppendVarBlockOne.itemCount);
		if (OurAppendVarBlockOne.itemCount == 0)
		{
			fprintf(stderr, "Item too long (#1)\n");
			exit(1);
		}
		bufferLen = VarBlockMakerFinish(
								&OurAppendVarBlockOne.varBlockMaker);	
		if (detail)
		{
			fprintf(stdout, 
				    "Finished VarBlock (length = %d, item count = %d)\n",
				    bufferLen, OurAppendVarBlockOne.itemCount);
		}
		
		BufferedAppendFinishBuffer(
					&OurAppendVarBlockOne.bufferedAppend, 
					bufferLen);
		
		OurAppendVarBlockOne.buffer = BufferedAppendGetMaxBuffer(
								&OurAppendVarBlockOne.bufferedAppend);
		
	    VarBlockMakerInit(
	        &OurAppendVarBlockOne.varBlockMaker,
	        OurAppendVarBlockOne.buffer,
	        OurAppendVarBlockOne.maxBufferLen,
	        tempSpace,
	        MAX_VARBLOCK_TEMPSPACE_LEN);
		
		OurAppendVarBlockOne.itemCount = 0;
		itemPtr = VarBlockMakerGetNextItemPtr(
								&OurAppendVarBlockOne.varBlockMaker,
								length);
		if (itemPtr == NULL)
		{
			fprintf(stderr, "Item too long (#2)\n");
			exit(1);
		}
	}

	OurAppendVarBlockOne.itemCount++;

	Assert(itemPtr != NULL);
	if (detail)
	{
		fprintf(stdout, 
			    "Copy into pointer %p\n",
			    itemPtr);
	}
	
	if (lineLength > 0)
		memcpy(itemPtr,line,length);
}

static void
doFinishAppendVarBlockOneContext(void)
{
	int32 bufferLen;
	int64 fileLen;
	int actualItemCount;

	actualItemCount = VarBlockMakerItemCount(&OurAppendVarBlockOne.varBlockMaker);
	Assert(actualItemCount == OurAppendVarBlockOne.itemCount);
	if (OurAppendVarBlockOne.itemCount > 0)
	{
		bufferLen = VarBlockMakerFinish(&OurAppendVarBlockOne.varBlockMaker);	
		if (detail)
		{
			fprintf(stdout, 
				    "Finished final VarBlock (length = %d, item count = %d)\n",
				    bufferLen, OurAppendVarBlockOne.itemCount);
		}
		BufferedAppendFinishBuffer(
					&OurAppendVarBlockOne.bufferedAppend, 
					bufferLen);
	}
	
	fileLen = BufferedAppendFileLen(&OurAppendVarBlockOne.bufferedAppend);
	if (detail || summary)
	{
		fprintf(stderr, "FileLen %lld\n", fileLen);
	}

	BufferedAppendCompleteFile(&OurAppendVarBlockOne.bufferedAppend);

	BufferedAppendFinish(&OurAppendVarBlockOne.bufferedAppend);
}

static void
doAppendVarBlock_One(File appendFile)
{
	doCreateAppendVarBlockOneContext(appendFile);
	
	while (true)
	{
		doReadLine();
		if (!endOfInput || lineLength > 0)
		{
			doAppendVarBlockOneLine(lineBuffer,lineLength);
		}

		if (endOfInput)
			break;
	}

	doFinishAppendVarBlockOneContext();

}

static void
doAppendRegular(File appendFile)
{
	int32 maxBufferLen;
	int32 memoryLen;
	uint8 *memory;
    BufferedAppend  bufferedAppend;
	uint8 *buffer;
	int64 fileLen;

	maxBufferLen = MAX_LINEBUFFER_LEN;
	
	memoryLen = BufferedAppendMemoryLen(maxBufferLen, largewrite);
	memory = (uint8*)malloc(memoryLen);
	if (memory == NULL)
	{
		fprintf(stderr, "Cannot allocate memory %d bytes", memoryLen);
		exit(1);
	}

	BufferedAppendInit(
		&bufferedAppend,
		memory,
		memoryLen,
		maxBufferLen,
		largewrite);

	BufferedAppendSetFile(
		&bufferedAppend,
		appendFile,
		0);
			
	while (true)
	{
		if (detail)
		{
			// UNDONE: Temporarily to stderr instead of stdout...
			printAppendHeader(stderr, &bufferedAppend);
		}
	
		doReadLine();
		if (!endOfInput || lineLength > 0)
		{
			if (detail)
			{
				fprintf(stderr, "Line: '%s', length %d\n", lineBuffer, lineLength);
			}
			buffer = BufferedAppendGetMaxBuffer(&bufferedAppend);

			if (lineLength > 0)
				memcpy(buffer,lineBuffer,lineLength);
			buffer[lineLength] = '\n';

			BufferedAppendFinishBuffer(&bufferedAppend, lineLength + 1);
		}

		if (endOfInput)
			break;
	}
	
	fileLen = BufferedAppendFileLen(&bufferedAppend);
	if (detail || summary)
	{
		fprintf(stderr, "FileLen %lld\n", fileLen);
	}

	BufferedAppendCompleteFile(&bufferedAppend);

	BufferedAppendFinish(&bufferedAppend);
}

static void
doAppendWordLength(File appendFile)
{
	int32 maxBufferLen;
	int32 memoryLen;
	uint8 *memory;
    BufferedAppend  bufferedAppend;
	uint8 *buffer;
	int32 bufferLen;
	int64 fileLen;
	int16 *length;

	maxBufferLen = MAX_LINEBUFFER_LEN + 2;
	
	memoryLen = BufferedAppendMemoryLen(maxBufferLen, largewrite);
	memory = (uint8*)malloc(memoryLen);
	if (memory == NULL)
	{
		fprintf(stderr, "Cannot allocate memory %d bytes", memoryLen);
		exit(1);
	}

	BufferedAppendInit(
		&bufferedAppend,
		memory,
		memoryLen,
		maxBufferLen,
		largewrite);

	BufferedAppendSetFile(
		&bufferedAppend,
		appendFile,
		0);
			
	while (true)
	{
		if (detail)
		{
			// UNDONE: Temporarily to stderr instead of stdout...
			printAppendHeader(stderr, &bufferedAppend);
		}
	
		doReadLine();
		if (!endOfInput || lineLength > 0)
		{
			bufferLen = 2 + ((lineLength + 1)/2)*2;
			if (detail)
			{
				fprintf(stderr, "Line: '%s', length %d (bufferLen %d)\n", lineBuffer, lineLength, bufferLen);
			}
			buffer = BufferedAppendGetBuffer(
									&bufferedAppend,
									bufferLen);
			
			length = (int16*)buffer;

			*length = lineLength;
			
			if (lineLength > 0)
				memcpy(buffer + 2,lineBuffer,lineLength);
			
			BufferedAppendFinishBuffer(
							&bufferedAppend, 
							bufferLen);
		}

		if (endOfInput)
			break;
	}
	
	fileLen = BufferedAppendFileLen(&bufferedAppend);
	if (detail || summary)
	{
		fprintf(stderr, "FileLen %lld\n", fileLen);
	}

	BufferedAppendCompleteFile(&bufferedAppend);

	BufferedAppendFinish(&bufferedAppend);
}

static void
doAppend(char* fname)
{
	int fileFlags = O_RDWR | PG_BINARY | O_TRUNC | O_CREAT;
	File appendFile;

	appendFile = open(fname, fileFlags, 0600);
	if (appendFile < 0)
	{
		perror(fname);
		exit(1);
	}

	if (compress_flavor == NO_COMPRESSION)
	{
		if (varblock)
		{
			if (one_flag)
				doAppendVarBlock_One(appendFile);
			else
				doAppendVarBlock(appendFile);
		}
		else if (word_flag)
			doAppendWordLength(appendFile);
		else
			doAppendRegular(appendFile);
	}
	else if (compress_flavor == LZJB || compress_flavor == ZLIB)
	{
		doAppendVarBlock(appendFile);
	}
	else
	{
		fprintf(stderr, "Compression flavor '%s' not supported here", compress_flavor_str);
		exit(1);
	}
}

static void
printReadHeader(FILE* where, BufferedRead  *bufferedRead)
{
	fprintf(where,"BufferedRead:\n");
	fprintf(where,"  Large-read memory level members:\n");
	fprintf(where,"    memory: (%p)\n", bufferedRead->memory);
	fprintf(where,"    memoryLen: %d\n", bufferedRead->memoryLen);
	fprintf(where,"    beforeBufferMemory: (%p)\n", bufferedRead->beforeBufferMemory);
	fprintf(where,"    largeReadMemory: (%p)\n", bufferedRead->largeReadMemory);
	fprintf(where,"    position: %lld\n", bufferedRead->position);
	fprintf(where,"    readLen: %d\n", bufferedRead->readLen);
	fprintf(where,"    maxBufferLen: %d\n", bufferedRead->maxBufferLen);
	fprintf(where,"    largeReadLen: %d\n", bufferedRead->largeReadLen);
	
	fprintf(where,"  Buffer level members:\n");
	fprintf(where,"    bufferOffset: %d\n", bufferedRead->bufferOffset);
	fprintf(where,"    bufferLen: %d\n", bufferedRead->bufferLen);
	
	fprintf(where,"  File level members:\n");
	fprintf(where,"    file: %d\n", bufferedRead->file);
	fprintf(where,"    fileLen: %lld\n", bufferedRead->fileLen);
}

static void
printVarBlockHeader(FILE* where, VarBlockReader *varBlockReader)
{
	int multiplier;
	VarBlockByteLen offsetArrayLen;
	
	fprintf(where,"VarBlockReader:\n");
	fprintf(where,"    header: (%p)\n", varBlockReader->header);
	fprintf(where,"        offsetsAreSmall: %s\n", (VarBlockGet_offsetsAreSmall(varBlockReader->header) ? "true" : "false"));
	fprintf(where,"        version: %d\n", VarBlockGet_version(varBlockReader->header));
	fprintf(where,"        itemLenSum: %d\n", VarBlockGet_itemLenSum(varBlockReader->header));
	fprintf(where,"        itemCount: %d\n", VarBlockGet_itemCount(varBlockReader->header));
	fprintf(where,"    bufferLen: %d\n", varBlockReader->bufferLen);
	fprintf(where,"    nextIndex: %d\n", varBlockReader->nextIndex);
	fprintf(where,"    offsetToOffsetArray: %d\n", varBlockReader->offsetToOffsetArray);
	fprintf(where,"    nextItemPtr: %p\n", varBlockReader->nextItemPtr);

	multiplier = (VarBlockGet_offsetsAreSmall(varBlockReader->header) ? 2 : 3);
	offsetArrayLen = VarBlockGet_itemCount(varBlockReader->header) * multiplier;
	fprintf(where,"    offsetArrayLen: %d\n", offsetArrayLen);
}

static void
doPrintBufferedReadSummary(BufferedRead *bufferedRead)
{
}

static void
doExtractVarBlockLines(
    VarBlockReader  *myVarBlockReader,
	uint8			*varBlockBuffer,
	int32			bufferLen)
{
	int itemCount;
    uint8 *itemPtr;
	int itemLen;
	int readerItemCount;
	int32 lineLen;

	/*
	 * Now use the VarBlock module to extract the lines out.
	 */
    VarBlockReaderInit(
        myVarBlockReader,
        varBlockBuffer,
        bufferLen);
	
	itemCount = 0;
	readerItemCount = VarBlockReaderItemCount(myVarBlockReader);
	
	if (detail)
	{
		printVarBlockHeader(stdout,myVarBlockReader);
	}

	while (true)
	{
		itemPtr = VarBlockReaderGetNextItemPtr(myVarBlockReader,&itemLen);
		if (itemPtr == NULL)
		{
			break;
		}
		if (detail)
		{
			fprintf(stdout, "Reader item count %d, item ptr %p, item length %d\n",
				    itemCount, itemPtr, itemLen);
		}

		if (heaptuple_flag)
		{
			Assert(itemLen > 0);
			doCopyOutOneColumnTextFromHeapTuple(
				itemPtr,
				itemLen,
				tempLineBuffer,
				MAX_LINEBUFFER_LEN,
				&lineLen);
			tempLineBuffer[lineLen] = '\0';
				
		}
		else if (minimaltuple_flag)
		{
			Assert(itemLen > 0);
			doCopyOutOneColumnTextFromMinimalTuple(
				itemPtr,
				itemLen,
				tempLineBuffer,
				MAX_LINEBUFFER_LEN,
				&lineLen);
			tempLineBuffer[lineLen] = '\0';
		}
		else
		{
			if (itemLen > 0)
			{
				memcpy(tempLineBuffer,itemPtr,itemLen);
			}
			tempLineBuffer[itemLen] = '\0';
		}
		if (detail)
		{
			fprintf(stdout, "Reader: '%s'\n", tempLineBuffer);
		}
		else
		{
			/*
			 * Here is where we copy the line to standard output.
			 */
			fprintf(stdout, "%s\n", tempLineBuffer);
		}
		itemCount++;
	}
	if (readerItemCount != itemCount)
	{
		printVarBlockHeader(stdout,myVarBlockReader);
		fprintf(stderr, "Reader count %d, found %d items\n",readerItemCount,itemCount);
		exit(1);
	}
}

static void
doReadVarBlocks(File readFile, int64 eof)
{
	int32 maxBufferLen;
	int32 actualMaxBufferLen;
	int32 memoryLen;
	uint8 *memory;
    BufferedRead bufferedRead;
	uint8 *buffer = NULL;
	int32 bufferLen;
	int32 availableLen = 0;
	int32 bufferCount = 0;
    VarBlockReader  myVarBlockReader;
	int32 lengthWordLen = sizeof(int32);
	int32 *lengthWord;
	int32 roundUpLen;
	int32 headerLen = sizeof(VarBlockHeader);
	VarBlockCheckError varBlockCheckError;

	maxBufferLen = MAX_VARBLOCK_BUFFER_LEN;	
	if (compress_flavor == LZJB || compress_flavor == ZLIB || length_flag)
	{
		actualMaxBufferLen = maxBufferLen + sizeof(int32);
		actualMaxBufferLen = ((actualMaxBufferLen + sizeof(int32) - 1)/sizeof(int32))*sizeof(int32);
	}
	else
		actualMaxBufferLen = maxBufferLen;

	memoryLen = BufferedReadMemoryLen(actualMaxBufferLen, largewrite);
	memory = (uint8*)malloc(memoryLen);
	if (memory == NULL)
	{
		fprintf(stderr, "Cannot allocate memory %d bytes", memoryLen);
		exit(1);
	}

	/*
	 * Initialize the BufferedRead module with a large amount of memory.
	 */
	BufferedReadInit(
		&bufferedRead,
		memory,
		memoryLen,
		actualMaxBufferLen,
		largewrite);

	/*
	 * Set the file to be read.
	 */
	BufferedReadSetFile(
		&bufferedRead,
		readFile,
		eof);
			
	while (true)
	{
		if (detail)
		{
			printReadHeader(stderr, &bufferedRead);
		}

		if (compress_flavor == NO_COMPRESSION && !length_flag)
		{
			/*
			 * Get a buffer of just the header in order to get the VarBlock length.
			 */
			buffer = BufferedReadGetNextBuffer(
									&bufferedRead,
									headerLen,
									&availableLen);
			if (buffer == NULL)
				break;
			
			bufferCount++;
			if (availableLen != headerLen)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Expected %d byte length (bufferCount %d)\n", 
					    headerLen, bufferCount);
				exit(1);
			}

			/*
			 * Ask the VarBlock module to use the header to how large the whole
			 * VarBlock is.
			 */
			bufferLen = VarBlockLenFromHeader(
											buffer,
											sizeof(VarBlockHeader));
			if (bufferLen <= headerLen)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Expected %d byte VarBlock length to be larger than %d header bytes (bufferCount %d)\n", 
					    bufferLen, headerLen, bufferCount);
				exit(1);
			}
			if (detail)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Calling BufferedReadGrowBuffer with %d for newMaxReadAheadLen (bufferCount %d)\n", 
					    bufferLen, bufferCount);
			}

			/*
			 * Grow the amount we can see just to the VarBlock length to avoid any
			 * unnecessary copying by BufferedRead.
			 */		 
			buffer = BufferedReadGrowBuffer(
									&bufferedRead,
									bufferLen,
									&availableLen);
			if (bufferLen != availableLen)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Wrong available length (expected %d byte length buffer and got %d (bufferCount %d)\n",
					    bufferLen, availableLen, bufferCount);
				exit(1);
			}

			doExtractVarBlockLines(
							&myVarBlockReader,
							buffer,
							bufferLen);	
		}
		else
		{
			/*
			 * Get a buffer of just the 32-bit data length.
			 */
			buffer = BufferedReadGetNextBuffer(
									&bufferedRead,
									lengthWordLen,
									&availableLen);
			if (buffer == NULL)
				break;
			
			bufferCount++;
			if (availableLen != lengthWordLen)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Expected %d byte length (bufferCount %d)\n", 
					    lengthWordLen, bufferCount);
				exit(1);
			}

			lengthWord = (int32*)buffer;
			bufferLen = *lengthWord;
			roundUpLen = sizeof(int32) + ((bufferLen + sizeof(int32) - 1)/sizeof(int32))*sizeof(int32);

			/*
			 * Grow the amount we can see just to the VarBlock length to avoid any
			 * unnecessary copying by BufferedRead.
			 */		 
			buffer = BufferedReadGrowBuffer(
									&bufferedRead,
									roundUpLen,
									&availableLen);
			if (roundUpLen != availableLen)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Wrong available length (expected %d byte length buffer and got %d (bufferCount %d)\n",
					    roundUpLen, availableLen, bufferCount);
				exit(1);
			}

			if (compress_flavor == NO_COMPRESSION)
			{
				uint8 *varBlock = buffer + sizeof(int32);
				int32 calculatedLen;

				Assert(length_flag);
				varBlockCheckError =VarBlockHeaderIsValid(
													varBlock,
													bufferLen);
				if (varBlockCheckError != VarBlockCheckOk)
				{
					printReadHeader(stderr, &bufferedRead);
					fprintf(stderr,"VarBlock header is not valid (bufferCount %d, detail = '%s')\n", 
						    bufferCount,
						    VarBlockGetCheckErrorStr());
					exit(1);
				}
				
				/*
				 * Ask the VarBlock module to use the header to how large the whole
				 * VarBlock is.
				 */
				calculatedLen = VarBlockLenFromHeader(
												varBlock,
												availableLen);
				if (bufferLen != calculatedLen)
				{
					printReadHeader(stderr, &bufferedRead);
					fprintf(stderr,"Expected %d byte VarBlock length to equal to %d bytes (bufferCount %d)\n", 
						    bufferLen, calculatedLen, bufferCount);
					exit(1);
				}
				if (detail)
				{
					printReadHeader(stderr, &bufferedRead);
					fprintf(stderr,"Calling BufferedReadGrowBuffer with %d for newMaxReadAheadLen (bufferCount %d)\n", 
						    bufferLen, bufferCount);
				}
				
				doExtractVarBlockLines(
								&myVarBlockReader,
								varBlock,
								bufferLen);	
			}
			else if (compress_flavor == LZJB || compress_flavor == ZLIB)
			{
				int32 compressedDataLen = bufferLen;
				int32 uncompressedDataLen;

				if (compress_flavor == ZLIB)
				{
#ifndef HAVE_LIBZ
					fprintf(stderr,"Don't have ZLIB on the system!");
					exit(1);
#else
					uncompressedDataLen = bufferedRead.maxBufferLen;		// Input to uncompress.  UNDONE: Use function to get value.
					if (uncompress(varBlockBuffer, (unsigned long*)&uncompressedDataLen, (Bytef *)(buffer + sizeof(int32)), (size_t)compressedDataLen) != Z_OK)
					{
						fprintf(stderr,"Uncompress failed");
						exit(1);
					}
#endif
				}
				else
				{
					uncompressedDataLen = (int32)lzjb_decompress(buffer + sizeof(int32), varBlockBuffer, (size_t)compressedDataLen, (size_t)maxBufferLen);
				}

				fprintf(stderr,"Decompressed length %d to VarBlock %d\n", compressedDataLen, uncompressedDataLen);

				varBlockCheckError =VarBlockHeaderIsValid(
													varBlockBuffer,
													uncompressedDataLen);
				if (varBlockCheckError != VarBlockCheckOk)
				{
					printReadHeader(stderr, &bufferedRead);
					fprintf(stderr,"VarBlock header is not valid (bufferCount %d, detail = '%s')\n", 
						    bufferCount,
						    VarBlockGetCheckErrorStr());
					exit(1);
				}
				
				/*
				 * Ask the VarBlock module to use the header to how large the whole
				 * VarBlock is.
				 */
				bufferLen = VarBlockLenFromHeader(
												varBlockBuffer,
												uncompressedDataLen);
				if (bufferLen != uncompressedDataLen)
				{
					printReadHeader(stderr, &bufferedRead);
					fprintf(stderr,"Expected %d byte VarBlock length to equal %d bytes (bufferCount %d)\n", 
						    bufferLen, uncompressedDataLen, bufferCount);
					exit(1);
				}
				doExtractVarBlockLines(
								&myVarBlockReader,
								varBlockBuffer,
								bufferLen);
			}
			else
			{
				fprintf(stderr, "Compression flavor '%s' not supported here", compress_flavor_str);
				exit(1);
			}
		}

	}


	if (detail || summary)
	{
		fprintf(stderr, "BufferCount %d\n", bufferCount);
	}

	doPrintBufferedReadSummary(&bufferedRead);
		
	BufferedReadCompleteFile(&bufferedRead);

	BufferedReadFinish(&bufferedRead);
}

typedef struct ReadVarBlockOneContext
{
	int32				 maxBufferLen;
	uint8				 *memory;
	int32				 memoryLen;
	BufferedRead         bufferedRead;
	bool				 haveVarBlock;
	uint8                *buffer;
	int32                bufferLen;
	int32                bufferCount;
	VarBlockReader       varBlockReader;
	int					 itemCount;
} ReadVarBlockOneContext;

static ReadVarBlockOneContext OurReadVarBlockOne;

static void
doCreateReadVarBlockOneContext(File readFile, int64 eof)
{
	OurReadVarBlockOne.maxBufferLen = MAX_VARBLOCK_BUFFER_LEN;	

	OurReadVarBlockOne.memoryLen = 
					BufferedReadMemoryLen(OurReadVarBlockOne.maxBufferLen,
					largewrite);
	OurReadVarBlockOne.memory = 
					(uint8*)malloc(OurReadVarBlockOne.memoryLen);
	if (OurReadVarBlockOne.memory == NULL)
	{
		fprintf(stderr, "Cannot allocate memory %d bytes", 
			    OurReadVarBlockOne.memoryLen);
		exit(1);
	}

	/*
	 * Initialize the BufferedRead module with a large amount of memory.
	 */
	BufferedReadInit(
		&OurReadVarBlockOne.bufferedRead,
		OurReadVarBlockOne.memory,
		OurReadVarBlockOne.memoryLen,
		OurReadVarBlockOne.maxBufferLen,
		largewrite);

	/*
	 * Set the file to be read.
	 */
	BufferedReadSetFile(
		&OurReadVarBlockOne.bufferedRead,
		readFile,
		eof);

	OurReadVarBlockOne.haveVarBlock = false;
	OurReadVarBlockOne.bufferLen = 0;
	OurReadVarBlockOne.bufferCount = 0;
}

static bool
doReadVarBlockOneBuffer(void)
{
	int32 headerLen = sizeof(VarBlockHeader);
	int32 availableLen = 0;
	int readerItemCount;
	VarBlockCheckError varBlockCheckError;
	
	if (detail)
	{
		printReadHeader(stderr, &OurReadVarBlockOne.bufferedRead);
	}

	/*
	 * Get a buffer of just the header in order to get the VarBlock length.
	 */
	OurReadVarBlockOne.buffer = BufferedReadGetNextBuffer(
							&OurReadVarBlockOne.bufferedRead,
							headerLen,
							&availableLen);
	if (OurReadVarBlockOne.buffer == NULL)
		return false;
	
	OurReadVarBlockOne.bufferCount++;
	if (availableLen != headerLen)
	{
		printReadHeader(stderr, &OurReadVarBlockOne.bufferedRead);
		fprintf(stderr,"Expected %d byte length (bufferCount %d)\n", 
			    headerLen, OurReadVarBlockOne.bufferCount);
		exit(1);
	}

	varBlockCheckError = VarBlockHeaderIsValid(
									OurReadVarBlockOne.buffer,
									availableLen);
	if (varBlockCheckError != VarBlockCheckOk)
	{
		printReadHeader(stderr, &OurReadVarBlockOne.bufferedRead);
		fprintf(stderr,"VarBlock header is not valid (bufferCount %d, detail = '%s')\n", 
			    OurReadVarBlockOne.bufferCount,
			    VarBlockGetCheckErrorStr());
		exit(1);
	}
		
	/*
	 * Ask the VarBlock module to use the header to how large the whole
	 * VarBlock is.
	 */
	OurReadVarBlockOne.bufferLen = VarBlockLenFromHeader(
										OurReadVarBlockOne.buffer,
										availableLen);
	if (OurReadVarBlockOne.bufferLen <= headerLen)
	{
		printReadHeader(stderr, &OurReadVarBlockOne.bufferedRead);
		fprintf(stderr,"Expected %d byte VarBlock length to be larger than %d header bytes (bufferCount %d)\n", 
			    OurReadVarBlockOne.bufferLen, headerLen, OurReadVarBlockOne.bufferCount);
		exit(1);
	}
	if (detail)
	{
		printReadHeader(stderr, &OurReadVarBlockOne.bufferedRead);
		fprintf(stderr,"Calling BufferedReadGrowBuffer with %d for newMaxReadAheadLen (bufferCount %d)\n", 
			    OurReadVarBlockOne.bufferLen, OurReadVarBlockOne.bufferCount);
	}

	/*
	 * Grow the amount we can see just to the VarBlock length to avoid any
	 * unnecessary copying by BufferedRead.
	 */		 
	OurReadVarBlockOne.buffer = BufferedReadGrowBuffer(
										&OurReadVarBlockOne.bufferedRead,
										OurReadVarBlockOne.bufferLen,
										&availableLen);
	if (OurReadVarBlockOne.bufferLen != availableLen)
	{
		printReadHeader(stderr, &OurReadVarBlockOne.bufferedRead);
		fprintf(stderr,"Wrong available length (expected %d byte length buffer and got %d (bufferCount %d)\n",
			    OurReadVarBlockOne.bufferLen, availableLen, OurReadVarBlockOne.bufferCount);
		exit(1);
	}
	
	/*
	 * Now use the VarBlock module to extract the lines out.
	 */
    VarBlockReaderInit(
        &OurReadVarBlockOne.varBlockReader,
        OurReadVarBlockOne.buffer,
        OurReadVarBlockOne.bufferLen);
	
	OurReadVarBlockOne.itemCount = 0;
	readerItemCount = VarBlockReaderItemCount(&OurReadVarBlockOne.varBlockReader);
	
	if (detail)
	{
		printVarBlockHeader(stdout,&OurReadVarBlockOne.varBlockReader);
	}

	return true;
}

static bool
doReadVarBlockOneLine(uint8 **line, int32 *length)
{
	bool gotBuffer;
    uint8 *itemPtr;
	int itemLen;
	
	if (!OurReadVarBlockOne.haveVarBlock)
	{
		gotBuffer = doReadVarBlockOneBuffer();
		if (!gotBuffer)
			return false;

		OurReadVarBlockOne.haveVarBlock = true;
	}

	itemPtr = VarBlockReaderGetNextItemPtr(&OurReadVarBlockOne.varBlockReader,&itemLen);
	if (itemPtr == NULL)
	{
		OurReadVarBlockOne.haveVarBlock = false;
		
		gotBuffer = doReadVarBlockOneBuffer();
		if (!gotBuffer)
			return false;

		OurReadVarBlockOne.haveVarBlock = true;

		itemPtr = VarBlockReaderGetNextItemPtr(&OurReadVarBlockOne.varBlockReader,&itemLen);
		if (itemPtr == NULL)
		{
			fprintf(stderr,"Expected at least one item from new VarBlock (bufferCount %d)\n", 
				    OurReadVarBlockOne.bufferCount);
			exit(1);
		}
	}
	if (detail)
	{
		fprintf(stdout, "Reader item count %d, item ptr %p, item length %d\n",
			    OurReadVarBlockOne.itemCount, itemPtr, itemLen);
	}
	OurReadVarBlockOne.itemCount++;

	*line = itemPtr;
	*length = itemLen;
	
	return true;

}

static void
doFinishReadVarBlockOne(void)
{
	if (detail || summary)
	{
		fprintf(stderr, "BufferCount %d\n", OurReadVarBlockOne.bufferCount);
	}

	doPrintBufferedReadSummary(&OurReadVarBlockOne.bufferedRead);
		
	BufferedReadCompleteFile(&OurReadVarBlockOne.bufferedRead);

	BufferedReadFinish(&OurReadVarBlockOne.bufferedRead);
}

static void
doReadVarBlocks_One(File readFile, int64 eof)
{
	bool gotLine;
	uint8 *line;
	int32 length;

	doCreateReadVarBlockOneContext(readFile, eof);
	
	while (true)
	{
		gotLine = doReadVarBlockOneLine(&line, &length);
		if (!gotLine)
			break;

		if (length > 0)
		{
			memcpy(tempLineBuffer,line,length);
			tempLineBuffer[length] = '\0';
			if (detail)
			{
				fprintf(stdout, "Reader: '%s'\n", tempLineBuffer);
			}
			else
			{
				/*
				 * Here is where we copy the line to standard output.
				 */
				fprintf(stdout, "%s\n", tempLineBuffer);
			}
		}
	}

	doFinishReadVarBlockOne();

}
static void
doReadRegular(File readFile, int64 eof)
{
	int32 maxBufferLen;
	int32 memoryLen;
	uint8 *memory;
    BufferedRead bufferedRead;
	uint8 *buffer = NULL;
	int32 availableLen = 0;
	int32 bufferCount = 0;
	int a;

	maxBufferLen = MAX_LINEBUFFER_LEN;

	memoryLen = BufferedReadMemoryLen(maxBufferLen, largewrite);
	memory = (uint8*)malloc(memoryLen);
	if (memory == NULL)
	{
		fprintf(stderr, "Cannot allocate memory %d bytes", memoryLen);
		exit(1);
	}

	/*
	 * Initialize the BufferedRead module with a large amount of memory.
	 */
	BufferedReadInit(
		&bufferedRead,
		memory,
		memoryLen,
		maxBufferLen,
		largewrite);

	/*
	 * Set the file to be read.
	 */
	BufferedReadSetFile(
		&bufferedRead,
		readFile,
		eof);
			
	while (true)
	{
		if (detail)
		{
			printReadHeader(stderr, &bufferedRead);
		}

		/*
		 * Get the maximum length buffer and process all the characters below.
		 */
		buffer = BufferedReadGetMaxBuffer(
								&bufferedRead,
								&availableLen);
		if (buffer == NULL)
			break;
		bufferCount++;
	
		if (detail || availableLen < 0 || availableLen > blocksize)
		{
			fprintf(stderr,"Available length %d (bufferCount %d)\n",availableLen, bufferCount);
		}
		Assert(availableLen > 0);
		Assert(availableLen <= blocksize);

		/*
		 * Process the available buffer char by char.
		 */		 
		for (a = 0; a < availableLen; a++)
		{
			if (lineLength >= MAX_LINEBUFFER_LEN)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"doRead: Line too long %d (bufferCount %d)\n",lineLength,bufferCount);
				exit(1);
			}
			if (buffer[a] == '\0')
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"doRead: NULL found (index %d, availableLen %d, bufferCount %d)\n",a, availableLen, bufferCount);
				exit(1);
			}
			lineBuffer[lineLength++] = buffer[a];
			if ((char)buffer[a] == '\n')
			{
				lineBuffer[lineLength++] = '\0';
			    fprintf(stdout,"%s",(char*)lineBuffer);
				lineLength = 0;
			}
		}
	}

	if (detail || summary)
	{
		fprintf(stderr, "BufferCount %d\n", bufferCount);
	}

	doPrintBufferedReadSummary(&bufferedRead);
		
	BufferedReadCompleteFile(&bufferedRead);

	BufferedReadFinish(&bufferedRead);
}

static void
doReadWordLength(File readFile, int64 eof)
{
	int32 maxBufferLen;
	int32 memoryLen;
	uint8 *memory;
    BufferedRead bufferedRead;
	uint8 *buffer = NULL;
	int32 bufferLen;
	int32 availableLen = 0;
	int32 bufferCount = 0;
	int16 *length_ptr;
	int16 length;
	int l;
	
	maxBufferLen = MAX_LINEBUFFER_LEN + 2;

	memoryLen = BufferedReadMemoryLen(maxBufferLen, largewrite);
	memory = (uint8*)malloc(memoryLen);
	if (memory == NULL)
	{
		fprintf(stderr, "Cannot allocate memory %d bytes", memoryLen);
		exit(1);
	}

	/*
	 * Initialize the BufferedRead module with a large amount of memory.
	 */
	BufferedReadInit(
		&bufferedRead,
		memory,
		memoryLen,
		maxBufferLen,
		largewrite);

	/*
	 * Set the file to be read.
	 */
	BufferedReadSetFile(
		&bufferedRead,
		readFile,
		eof);
			
	while (true)
	{
		if (detail)
		{
			printReadHeader(stderr, &bufferedRead);
		}

		buffer = BufferedReadGetNextBuffer(
								&bufferedRead,
								2,
								&availableLen);
		if (buffer == NULL)
			break;
		bufferCount++;
		if (availableLen != 2)
		{
			printReadHeader(stderr, &bufferedRead);
			fprintf(stderr,"Expected 2 byte length (bufferCount %d)\n", bufferCount);
			exit(1);
		}
		length_ptr = (int16*)buffer;
		/*
		 * Must capture value from buffer now before we call GetNextBuffer again!!!
		 */
		length = *length_ptr;

		Assert(length >= 0);

		if (detail)
		{
			printReadHeader(stderr, &bufferedRead);
			fprintf(stderr,"line length %d (bufferCount %d)\n", length, bufferCount);
		}
			
		if (length > 0)
		{
			if (length > MAX_LINEBUFFER_LEN)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"line too long (length %d, bufferCount %d)\n",length, bufferCount);
				exit(1);
			}

			bufferLen = ((length+1)/2)*2;
			buffer = BufferedReadGetNextBuffer(
									&bufferedRead,
									bufferLen,
									&availableLen);

			if (buffer == NULL)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Reached end and expected %d byte length buffer (bufferCount %d)\n", bufferLen, bufferCount);
				exit(1);
			}
			if (bufferLen != availableLen)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Wrong available length (expected %d byte length buffer and got %d (bufferCount %d)\n", bufferLen, availableLen, bufferCount);
				exit(1);
			}

			Assert(buffer >= memory);
			Assert(buffer < memory + memoryLen);
			if (detail)
			{
				printReadHeader(stderr, &bufferedRead);
				fprintf(stderr,"Got bufferLen %d (tempLineBuffer %p, buffer %p, length %d, bufferCount %d)\n", 
					    bufferLen, tempLineBuffer, buffer, length, bufferCount);
			}
			memcpy(tempLineBuffer, buffer, length);
			for (l = 0; l < length; l++)
			{
				if (tempLineBuffer[l] == '\0')
				{
					printReadHeader(stderr, &bufferedRead);
					fprintf(stderr,"doRead: NULL found (index %d, length %d, bufferCount %d)\n",l, length, bufferCount);
					exit(1);
				}
			}
		}
		tempLineBuffer[length] = '\0';
		
	    fprintf(stdout,"%s\n",(char*)tempLineBuffer);
	}

	if (detail || summary)
	{
		fprintf(stderr, "BufferCount %d\n", bufferCount);
	}

	doPrintBufferedReadSummary(&bufferedRead);
		
	BufferedReadCompleteFile(&bufferedRead);

	BufferedReadFinish(&bufferedRead);
}

static void
doRead(char* fname)
{
	int fileFlags = O_RDONLY | PG_BINARY;
	File readFile;
	int64 eof;

	readFile = open(fname, fileFlags, 0);
	if (readFile < 0)
	{
		perror(fname);
		exit(1);
	}
	eof = lseek(readFile, 0, SEEK_END);
	if (eof < 0)
	{
		fprintf(stderr, "Cannot seek to end (error %d)", errno);
		exit(1);
	}
	if (summary)
	{
		fprintf(stderr, "EOF %lld\n", eof);
	}
	if (lseek(readFile, 0, SEEK_SET) < 0)
	{
		fprintf(stderr, "Cannot seek to beginning (error %d)", errno);
		exit(1);
	}

	if (varblock || compress_flag)
	{
		if (one_flag)
		{
			Assert(!compress_flag);		// Not implemented.
			doReadVarBlocks_One(readFile,eof);
		}
		else
			doReadVarBlocks(readFile,eof);
	}
	else if (word_flag)
	{
		doReadWordLength(readFile,eof);
	}
	else
	{
		doReadRegular(readFile,eof);
	}
}

/* 
 * Exit closing files
 */
static void
exit_gracefuly(int status)
{
//	close(binaryFd);
	exit(status);
}


static void
help(void)
{
	printf("bufferedtest ... \n\n");
	printf("Usage:\n");
	printf("  bufferedtest [OPTION]... \n");
	printf("\nOptions controlling the output content:\n");
	printf("  -a, --append              Take standard input and use BufferedAppend to write a file.\n"); 
	printf("  -r, --read                Use BufferedRead to read a file and send it to standard output.\n"); 
	printf("  -w, --word                Use 16-bit word-lengths.\n"); 
	printf("  -d, --detail              Display detailed information.\n");
	printf("  -s, --summary             Display summary information.\n");
	printf("  -v, --varblock            Pack lines into VarBlocks.\n");
	printf("  -o, --one                 Use one-at-a-time style.\n");
	printf("  -c, --compress            Use compression.\n");
	printf("  -l, --length              Use 32-bit length words for non-block-compressed VarBlocks.\n");
	printf("  -h, --heaptuple           Make heaptuples (one column text table).\n");
	printf("  -m, --minimaltuple        Make minimaltuples (one column text table).\n");
	exit(0);
}

int
main(int argc, char** argv)
{
	int	c, optindex;
	int s;

	static struct option long_options[] = {
		{"append", no_argument, NULL, 'a'},
		{"read", no_argument, NULL, 'r'},
		{"word", no_argument, NULL, 'w'},
		{"detail", no_argument, NULL, 'd'},
		{"summary", no_argument, NULL, 's'},
		{"varblock", no_argument, NULL, 'v'},
		{"one", no_argument, NULL, 'o'},
		{"compress", required_argument, NULL, 'c'},
		{"length", no_argument, NULL, 'l'},
		{"heaptuple", no_argument, NULL, 'h'},
		{"minimaltuple", no_argument, NULL, 'm'},
		{"gdb", no_argument, NULL, 'g'},
		{"help", no_argument, NULL, '?'},
		{NULL, 0, NULL, 0}
	};
	
	if (argc == 1 || !strcmp(argv[1], "--help") || !strcmp(argv[1], "-?"))
		help();

	while ((c = getopt_long(argc, argv, "argwdsvoc:lhm",
							long_options, &optindex)) != -1)
	{
		switch (c)
		{
			case 'a':			/* append */
				append = true;
				break;
			case 'r':			/* read */
				read_flag = true;
				break;
			case 'g':			/* gdb */
				gdb_flag = true;
				for (s = 0; s < 15; s++)
				{
				    sleep(1);
					fprintf(stdout,"sleep 1 for debug\n");
				}
				break;
			case 'w':			/* (length-)word */
				word_flag = true;
				break;
			case 'd':			
				detail = true;	/* detail */
				break;
			case 's':			
				summary = true;	/* summary */
				break;
			case 'v':			
				varblock = true;	/* varblock */
				break;
			case 'o':			
				one_flag = true;	/* one */
				break;
			case 'c':	
				{
					int len;
					compress_flag = true;	/* compress */

					compress_flavor_str = optarg;

					if (compress_flavor_str == NULL)
						compress_flavor = LZJB;
					else
					{
						len = strlen(compress_flavor_str);
						if (len == strlen("lzjb") &&
							strcmp(compress_flavor_str, "lzjb") == 0)
							compress_flavor = LZJB;
						else if (len == strlen("zlib") &&
							strcmp(compress_flavor_str, "zlib") == 0)
						{
#ifndef HAVE_LIBZ
							fprintf(stderr,"Don't have ZLIB on the system!");
							exit(1);
#endif
							compress_flavor = ZLIB;
						}
						else
						{
							fprintf(stderr,"Unknown compress flavor '%s'\n", compress_flavor_str);
							exit(1);
						}
					}
					
				}
				break;
			case 'l':			
				length_flag = true;	/* length */
				break;
			case 'h':			
				heaptuple_flag = true;	/* heaptuple */
				break;
			case 'm':			
				minimaltuple_flag = true;	/* minimaltuple */
				break;
			default:
				fprintf(stderr, "Try \"bufferedtest --help\" for more information.\n");
				exit(1);
		}
	}


// #ifdef HAVE_LIBZ
//	fprintf(stderr,"We have LIBZ\n");
// #endif

	if (append)
	{
		char *fname;
		
		if (optind >= argc)
		{
			fprintf(stderr, "Missing file specification.\n");
			exit(1);
		}
		
		fname = argv[optind];

		doAppend(fname);
		exit_gracefuly(0);
	}

	if (read_flag)
	{
		char *fname;
		
		if (optind >= argc)
		{
			fprintf(stderr, "Missing file specification.\n");
			exit(1);
		}
		
		fname = argv[optind];

		doRead(fname);
		exit_gracefuly(0);
	}

/*
	if (reader)
	{
		char *fname;
		
		if (optind >= argc)
		{
			fprintf(stderr, "Missing file specification.\n");
			exit(1);
		}
		
		fname = argv[optind];
		binaryFd = open(fname, O_RDONLY | PG_BINARY, 0);

		if (binaryFd < 0)
		{
			perror(fname);
			exit(1);
		}
		doReader(fname);
		exit_gracefuly(0);
	}
*/
	exit_gracefuly(0);
	
	/* just to avoid a warning */
	return 0;
}

/*
 * Routines needed if headers were configured for ASSERT
 */
#ifndef assert_enabled
bool		assert_enabled = true;
#endif

int
ExceptionalCondition(const char *conditionName,
					 const char *errorType,
					 const char *fileName,
					 int         lineNumber)
{
	fprintf(stderr, "TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n",
			errorType, conditionName,
			fileName, lineNumber);

	abort();
	return 0;
}
