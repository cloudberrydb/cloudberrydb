#ifndef _UTILS_H_
#define _UTILS_H_

#include "gopher/gopher.h"

#include "src/provider/common/config.h"
#include "src/dlproxy/uriparser.h"
#include "utils/plancache.h"

struct List;
struct FileScanTask;
struct ExternalTableMetadata;

#define BLOCK_SIZE (1024 * 1024 * 8)

#define SET_RECORD_IS_DELETED(recordWrapper) ((recordWrapper)->record.position = -1)
#define SET_RECORD_IS_SKIPPED(recordWrapper) ((recordWrapper)->record.position = -2)
#define RECORD_IS_DELETED(recordWrapper) ((recordWrapper)->record.position == -1)
#define RECORD_IS_SKIPPED(recordWrapper) ((recordWrapper)->record.position == -2)

typedef struct ReaderInitInfo
{
	int				taskId;
	MemoryContext	mcxt;
	List		   *datafileDesc;
	bool		   *attrUsed;
	gopherFS		gopherFilesystem;
	FileScanTask   *fileScanTask;
	ExternalTableMetadata *tableOptions;
} ReaderInitInfo;

typedef struct InternalRecord
{
	Datum    *values;
	bool     *nulls;
	int64     position;
} InternalRecord;

typedef struct InternalRecordWrapper {
	InternalRecord  record;
	void           *recordDesc;
} InternalRecordWrapper;

typedef struct InternalRecordDesc {
	void *hashTab;
	int   nColumns;
	int   nKeys;
	int  *keyIndexes;
	Oid  *attrType;
	bool *attrUsed;
} InternalRecordDesc;

typedef struct Reader
{
	struct Reader *(*Create) (void *args);
	bool (*Next) (struct Reader *reader, InternalRecord *record);
	void (*Close) (struct Reader *reader);
} Reader;

typedef struct FieldDescription
{
	char  name[NAMEDATALEN];
	Oid   typeOid;
	int   typeMod;
} FieldDescription;

typedef struct KeyValue
{
	char *key;
	char *value;
} KeyValue;

extern PGDLLIMPORT int PostPortNumber;

typedef struct RowReader
{
	List					*fileScanTasks;
	Reader					*curReader;
	List					*datafileDesc;
	bool					*attrUsed;
	gopherFS				gopherFilesystem;
	MemoryContext			mcxt;
	int						curReaderIndex;
	Reader					*handler;
	char					format;
	ExternalTableMetadata	*tableOptions;
	MemoryContext			taskMcxt;
	MemoryContext			curMcxt;
} RowReader;

typedef struct RemoteFileHandle
{
	gopherFS        gopherFilesystem;
	RowReader      *reader;
	ResourceOwner   owner;
	struct RemoteFileHandle *next;
	struct RemoteFileHandle *prev;
} RemoteFileHandle;

typedef struct ProtocolContext
{
	List			*filterQuals;
	MemoryContext	rowContext;
	InternalRecord	*record;
	RemoteFileHandle *file;
} ProtocolContext;

#ifndef WORDS_BIGENDIAN
static inline uint32_t
reverse32(uint32_t value)
{
	value = (value >> 16) | (value << 16);
	return ((value & 0xff00ff00UL) >> 8) | ((value & 0x00ff00ffUL) << 8);
}

static inline uint64_t
reverse64(uint64_t value)
{
	value = (value >> 32) | (value << 32);
	value = ((value & 0xff00ff00ff00ff00ULL) >> 8) | ((value & 0x00ff00ff00ff00ffULL) << 8);
	return ((value & 0xffff0000ffff0000ULL) >> 16) | ((value & 0x0000ffff0000ffffULL) << 16);
}
#endif

char *tolowercase(const char *input, char *output);
char *splitPath(char *filePath);
List *createFieldDescription(TupleDesc tupleDesc);
List *createPositionDeletesDescription(void);
int64 getFileRecordCount(List *deletes);
bool charSeqEquals(char *s1, int s1Len, char *s2, int s2Len);
int charSeqIndexOf(char *array, int arrayLength, char *target, int targetLength);
int *createRecordKeyIndexes(List *recordKeys, List *columnDesc);
uint32 fieldHash(Datum datum, Oid type);
bool fieldCompare(Datum datum1, Datum datum2, Oid type);
void deepCopyField(Datum *datum, Oid type, int index);
InternalRecordWrapper *createInternalRecordWrapper(void *recordDesc, int nColumns);
void destroyInternalRecordWrapper(InternalRecordWrapper *recordWrapper);
bool *createDeleteProjectionColumns(List *recordKeys, List *columnDesc);
uint32 recordHash(const void *key, Size keysize);
int recordMatch(const void *key1, const void *key2, Size keysize);
void deepCopyRecord(InternalRecordWrapper *recordWrapper);
int extractScalFromTypeMod(int32 typmod);
List *extractRecordKeyValues(char *strRecordKey, int strSize, int keySize);
Datum createDatumByText(Oid attType, const char *value);
List *extractPartitionKeyValues(char *filePaht, List *partitionKeys, bool isHiveStyle);
void freeKeyValueList(List *kvs);

#endif /* _UTILS_H_ */
