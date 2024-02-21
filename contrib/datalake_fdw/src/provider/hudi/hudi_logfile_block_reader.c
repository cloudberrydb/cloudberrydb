#include "postgres.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "src/dlproxy/datalake.h"
#include "gopher/gopher.h"
#include "hudi_logfile_block_reader.h"
// #include "datalake_extension.h"

#define DEFAULT_VERSION 0
#define BLOCK_SCAN_READ_BUFFER_SIZE 1024 * 1024

static void freeBlockMetadata(HudiMetadataElem *metadata);

static char MAGIC[] = {'#', 'H', 'U', 'D', 'I', '#'};
static char* blockTypeNames[] = {"COMMAND_BLOCK",
								 "DELETE_BLOCK",
								 "CORRUPTED_BLOCK",
								 "AVRO_BLOCK",
								 "HFILE_BLOCK",
								 "PARQUET_BLOCK",
								 "CDC_BLOCK"};

static inline bool
logFileSeek(HudiLogFileReader *reader, int64_t offset, bool supressError)
{
	if (gopherSeek(reader->gopherFilesystem, reader->gopherFile, offset) == -1)
	{
		if (!supressError)
			elog(ERROR, "failed to seek hudi logfile \"%s\": %s", reader->fileName, gopherGetLastError());

		return false;
	}

	reader->offset = offset;
	return true;
}

static inline void
logFileOpen(HudiLogFileReader *reader, int flag)
{
	char *filePath;;

	filePath = splitPath(reader->fileName);

	reader->gopherFile = gopherOpenFile(reader->gopherFilesystem, filePath, flag, BLOCK_SIZE);
	if (reader->gopherFile == NULL)
		elog(ERROR, "failed to open hudi logfile \"%s\": %s", reader->fileName, gopherGetLastError());

	pfree(filePath);
}

static inline int
logFileRead(HudiLogFileReader *reader, char *buffer, int bufferSize)
{
	int bytesRead = gopherRead(reader->gopherFilesystem, reader->gopherFile, buffer, bufferSize);
	if (bytesRead == -1)
		elog(ERROR, "failed to read hudi logfile \"%s\": %s", reader->fileName, gopherGetLastError());

	reader->offset += bytesRead;
	return bytesRead;
}

static inline int64_t
logFileReadBigInteger(HudiLogFileReader *reader, bool *isReachedEOF)
{
	int bytesRead;
	int64_t result;

	if (isReachedEOF)
		*isReachedEOF = false;

	bytesRead = logFileRead(reader, (char *) &result, sizeof(result));
	if (bytesRead == 0)
	{
		if (!isReachedEOF)
			elog(ERROR, "failed to read int64 from \"%s\": read past EOF", reader->fileName);

		*isReachedEOF = true;
	}

#ifndef WORDS_BIGENDIAN
	result = reverse64(result);
#endif

	return result;
}

static inline int
logFileReadInteger(HudiLogFileReader *reader, bool *isReachedEOF)
{
	int bytesRead;
	int result;

	if (isReachedEOF)
		*isReachedEOF = false;

	bytesRead = logFileRead(reader, (char *) &result, sizeof(result));
	if (bytesRead == 0)
	{
		if (!isReachedEOF)
			elog(ERROR, "failed to read int32 from \"%s\": read past EOF", reader->fileName);

		*isReachedEOF = true;
	}

#ifndef WORDS_BIGENDIAN
	result = reverse32(result);
#endif

	return result;
}

static bool
readMagic(HudiLogFileReader *reader, bool *isCorrupted)
{
	int bytesRead;
	char magicBuffer[sizeof(MAGIC)];

	bytesRead = logFileRead(reader, magicBuffer, sizeof(MAGIC));
	if (bytesRead == 0)
	{
		*isCorrupted = false;
		return false;
	}

	if (memcmp(magicBuffer, MAGIC, sizeof(MAGIC)) != 0)
	{
		*isCorrupted = true;
		return false;
	}

	*isCorrupted = false;
	return true;
}

static int64_t
scanForNextAvailableBlockOffset(HudiLogFileReader *reader)
{
	int bytesRead;
	int64_t pos;
	int64_t currentPos;
	bool eof = false;
	char *dataBuf = palloc0(BLOCK_SCAN_READ_BUFFER_SIZE);

	while(true)
	{
		currentPos = reader->offset;

		bytesRead = logFileRead(reader, dataBuf, BLOCK_SCAN_READ_BUFFER_SIZE);
		if (bytesRead == 0)
			eof = true;

		pos = charSeqIndexOf(dataBuf, BLOCK_SCAN_READ_BUFFER_SIZE, MAGIC, sizeof(MAGIC));
		if (pos >= 0)
		{
			pfree(dataBuf);
			return currentPos + pos;
		}

		if (eof)
		{
			pfree(dataBuf);
			return reader->offset;
		}

		logFileSeek(reader, currentPos + BLOCK_SCAN_READ_BUFFER_SIZE - sizeof(MAGIC), false);
	}
}

static char *
tryReadContent(HudiLogFileReader *reader, int contentLength)
{
	char *content;

	if (contentLength == 0)
		return NULL;

	content = palloc(contentLength);
	logFileRead(reader, content, contentLength);
	return content;
}

static HudiLogFileCorruptBlock *
createCorruptBlock(HudiLogFileReader *reader, int64_t blockStartPos)
{
	int corruptedBlockSize;
	int64_t nextBlockOffset;
	char *corruptedBytes;
	HudiLogFileCorruptBlock *block;

	elog(DEBUG1, "logfile \"%s\" has a corrupted block at" INT64_FORMAT, reader->fileName, blockStartPos);

	logFileSeek(reader, blockStartPos, false);
	nextBlockOffset = scanForNextAvailableBlockOffset(reader);

    /* Rewind to the initial start and read corrupted bytes till the nextBlockOffset */
	logFileSeek(reader, blockStartPos, false);
	elog(DEBUG1, "next available block in \"%s\" starts at" INT64_FORMAT, reader->fileName, nextBlockOffset);

	corruptedBlockSize = (int) (nextBlockOffset - blockStartPos);
	corruptedBytes = tryReadContent(reader, corruptedBlockSize);

	block = palloc(sizeof(HudiLogFileCorruptBlock));

	block->type = CORRUPTED_BLOCK;
	block->length = corruptedBlockSize;
	block->content = corruptedBytes;

	return block;
}

static HudiLogFileDataBlock *
createDataBlock(HudiLogBlockType blockType,
				int contentLength,
				char *content,
				HudiMetadataElem *header)
{
	HudiLogFileDataBlock *block = palloc(sizeof(HudiLogFileDataBlock));

	block->type = blockType;
	block->length = contentLength;
	block->content = content;
	block->header = header;

	return block;
}

static bool
isBlockCorrupted(HudiLogFileReader *reader, int blockSize)
{
	int64_t currentPos = reader->offset;
	int64_t blockSizeFromFooter;
	bool isReachedEOF;
	bool isSucced;
	bool isCorrupted;

	/* 
	 * skip block corrupt check if writes are transactional. see https://issues.apache.org/jira/browse/HUDI-2118
	 */

	
	/*
	 * check if the blocksize mentioned in the footer is the same as the header;
	 * by seeking and checking the length of a long.  We do not seek `currentPos + blocksize`
	 * which can be the file size for the last block in the file, causing end-of-file error
	 */
	isReachedEOF = !logFileSeek(reader, currentPos + blockSize - sizeof(int64_t), true);

	/*
	 * Block size in the footer includes the magic header, which the header does not include.
	 * So we have to shorten the footer block size by the size of magic hash
	 */
	if (!isReachedEOF)
		blockSizeFromFooter = logFileReadBigInteger(reader, &isReachedEOF) - sizeof(MAGIC);

	if (isReachedEOF)
	{
		elog(LOG, "found corrupted block in logfile \"%s\"  with block size(%d) running past EOF",
				reader->fileName, blockSize);

		logFileSeek(reader, currentPos, false);
		return true;
	}

	if (blockSize != blockSizeFromFooter)
	{
		elog(LOG, "found corrupted block in logfile \"%s\". Header block size(%d) did not match"
				  " the footer block size(%ld)", reader->fileName, blockSize, blockSizeFromFooter);
		logFileSeek(reader, currentPos, false);
		return true;
	}

	isSucced = readMagic(reader, &isCorrupted);
	if (!isSucced && isCorrupted)
	{
		elog(LOG, "found corrupted block in logfile \"%s\". No magic hash "
				  "found right after footer block size entry", reader->fileName);
		logFileSeek(reader, currentPos, false);
		return true;
	}

	logFileSeek(reader, currentPos, false);
	return false;
}

static HudiMetadataElem *
getLogMetadata(HudiLogFileReader *reader)
{
	int metadataCount;
	int metadataEntryIndex;
	int metadataEntrySize;
	char *metadataEntry;
	int bytesRead;
	HudiMetadataElem *result = NULL;

	metadataCount = logFileReadInteger(reader, NULL);
	result = palloc0(sizeof(HudiMetadataElem) * METADATA_TYPE_MAX);

	while (metadataCount)
	{
		metadataEntryIndex = logFileReadInteger(reader, NULL);
		metadataEntrySize = logFileReadInteger(reader, NULL);

		metadataEntry = palloc(metadataEntrySize + 1);
		bytesRead = logFileRead(reader, metadataEntry, metadataEntrySize);
		if (bytesRead == 0)
			elog(ERROR, "failed to read metadata of block \"%s\": read past EOF", reader->fileName);

		metadataEntry[metadataEntrySize] = '\0';

		if (metadataEntryIndex < INSTANT_TIME || metadataEntryIndex > COMPACTED_BLOCK_TIMES)
			elog(ERROR, "failed to parse metadata of blcok \"%s\": invalid metadata type %d",
						reader->fileName, metadataEntryIndex);

		result[metadataEntryIndex].value = metadataEntry;

		metadataCount--;
	}

	return result;
}

static inline bool
logFileHasFooter(int version)
{
	switch(version)
	{
		case DEFAULT_VERSION:
			return false;
		case 1:
			return true;
		default:
			return false;
	}
}

#define logFileHasLogBlockLength(version) logFileHasFooter((version))

static inline bool
logFileHasHeader(int version)
{
	switch(version)
	{
		case DEFAULT_VERSION:
			return false;
		default:
			return true;
	}
}

static HudiLogFileBlock * 
readBlock(HudiLogFileReader *reader)
{
	int blockSize;
	int64_t blockStartPos = reader->offset;
	bool isCorrupted;
	bool isReachedEOF;
	int nextBlockVersion;
	HudiLogBlockType blockType;
	HudiMetadataElem *header;
	HudiMetadataElem *footer;
	int contentLength;
	char *content;

	/* 1 Read the total size of the block */
	blockSize = logFileReadBigInteger(reader, &isReachedEOF);

	/*
	 * An exception reading any of the above indicates a corrupt block
     * Create a corrupt block by finding the next MAGIC marker or EOF
	 */
	if (isReachedEOF)
		return (HudiLogFileBlock *) createCorruptBlock(reader, blockStartPos);

	/*
	 * We may have had a crash which could have written this block partially
	 * Skip blockSize in the stream and we should either find a sync marker (start of the next
	 * block) or EOF. If we did not find either of it, then this block is a corrupted block.
	 */
	isCorrupted = isBlockCorrupted(reader, blockSize);
	if (isCorrupted)
		return (HudiLogFileBlock *) createCorruptBlock(reader, blockStartPos);

	/* 2. Read the version for this log format */
	nextBlockVersion = logFileReadInteger(reader, NULL);

	/* 3. Read the block type for a log block */
	blockType = logFileReadInteger(reader, NULL);

	/* 4. Read the header for a log block, if present */
	header = logFileHasHeader(nextBlockVersion) ? getLogMetadata(reader) : NULL;

	/* 5. Read the content length for the content */
	contentLength = logFileReadBigInteger(reader, NULL);

	/* 6. Read the content */
	content = tryReadContent(reader, contentLength);

	/* 7. Read footer if any */
	footer = logFileHasFooter(nextBlockVersion) ? getLogMetadata(reader) : NULL;
	if (footer)
		freeBlockMetadata(footer);

	/* 8. Read log block length, if present. This acts as a reverse pointer when traversing a log file in reverse */
	if (logFileHasLogBlockLength(nextBlockVersion))
		logFileReadBigInteger(reader, NULL);

	switch (blockType)
	{
		case AVRO_BLOCK:
		case DELETE_BLOCK:
		case COMMAND_BLOCK:
			return (HudiLogFileBlock *) createDataBlock(blockType,
														contentLength,
														content,
														header);
		case HFILE_BLOCK:
			elog(ERROR, "Unsupported Block: HFILE DATA BLOCK");
			break;
		case PARQUET_BLOCK:
			elog(ERROR, "Unsupported Block: PARQUET DATA BLOCK");
			break;
		case CDC_BLOCK:
			elog(ERROR, "Unsupported Block: CDC DATA BLOCK");
			break;
		default:
			elog(ERROR, "Unsupported Block: %d", blockType);
	}

	return NULL;
}

static void
freeBlockMetadata(HudiMetadataElem *metadata)
{
	int i;

	for(i = 0; i < METADATA_TYPE_MAX; i++)
	{
		if (metadata[i].value)
			pfree(metadata[i].value);
	}

	pfree(metadata);
}

void
freeLogBlock(HudiLogFileBlock *block)
{
	HudiLogFileDataBlock *dataBlock = (HudiLogFileDataBlock *) block;

	if (dataBlock->content)
		pfree(dataBlock->content);

	if (dataBlock->type == CORRUPTED_BLOCK)
		goto done;

	if (dataBlock->header != NULL)
		freeBlockMetadata(dataBlock->header);

done:
	pfree(dataBlock);
}

const char *
logBlockGetTypeName(HudiLogFileBlock *block)
{
	return blockTypeNames[block->type];
}

static const char *
getHeaderOption(HudiLogFileBlock *block, HeaderMetadataType index)
{
	HudiLogFileDataBlock *dataBlock = (HudiLogFileDataBlock *) block;

	if (block->type == CORRUPTED_BLOCK)
		return NULL;

	return dataBlock->header[index].value;
}

const char *
logBlockGetInstantTime(HudiLogFileBlock *block)
{
	return getHeaderOption(block, INSTANT_TIME);
}

const char *
logBlockGetTargetInstantTime(HudiLogFileBlock *block)
{
	return getHeaderOption(block, TARGET_INSTANT_TIME);
}

const char *
logBlockGetSchema(HudiLogFileBlock *block)
{
	return getHeaderOption(block, SCHEMA);
}

HudiLogFileReader *
createHudiLogFileReader(MemoryContext mcxt, gopherFS gopherFilesystem, char *fileName)
{
	HudiLogFileReader *reader = palloc(sizeof(HudiLogFileReader));

	reader->mcxt = mcxt;
	reader->gopherFilesystem = gopherFilesystem;
	reader->gopherFile = NULL;
	reader->fileName = fileName;
	reader->offset = 0;

	hudiLogFileOpen(reader);

	return reader;
}

void
hudiLogFileOpen(HudiLogFileReader *reader)
{
	int flag = O_RDONLY;

	/*
	 * TODO: Determine whether to cache a file based on DFS type, for now, we only support hdfs.
	 * Hoodie on hdfs will append the delta log file when executing write operation on MOR table,
	 * so we have to disable cache-file in gopher
	 */
	flag |= O_RDTHR;

	logFileOpen(reader, flag);
	logFileSeek(reader, 0, false);
}

bool
hudiLogFileNext(HudiLogFileReader *reader, HudiLogFileBlock **logBlock)
{
	bool isCorrupted;
	bool hasNext;
	MemoryContext oldContext;

	hasNext = readMagic(reader, &isCorrupted);
	if (!hasNext)
	{
		if (!isCorrupted)
			return false;

		elog(ERROR, "found corrupted block in logfile \"%s\". No magic hash "
					"found right after footer block size entry", reader->fileName);
	}

	oldContext = MemoryContextSwitchTo(reader->mcxt);
	*logBlock = readBlock(reader);
	MemoryContextSwitchTo(oldContext);

	return true;
}

void
hudiLogFileClose(HudiLogFileReader *reader)
{
	pfree(reader->fileName);

	if (reader->gopherFile)
		gopherCloseFile(reader->gopherFilesystem, reader->gopherFile, true);

	pfree(reader);
}
