#ifndef DATALAKE_REWRLOGICAL_H
#define DATALAKE_REWRLOGICAL_H

#include <vector>
#include <set>
#include "dataBufferArray.h"
#include "readPolicy.h"
#include "fileSystemWrapper.h"
#include "src/provider/provider.h"

extern "C" {
#include "src/datalake_def.h"
#include "src/common/partition_selector.h"
}

namespace Datalake {
namespace Internal {

#define READ_BATCH_SIZE (1000)
#define BLOCK_POLICY_SIZE (16 * 1024 * 1024)

#define SUPPORT_ENABLE_CACHE(option) \
	((option.enableCache == true) && (option.ptype == PROTOTCOL_HDFS)) \


typedef enum fileState
{
    FILE_OPEN = 0,
    FILE_CLOSE,
    FILE_UNDEF
}fileState;

typedef enum protocolType
{
	PROTOTCOL_HDFS = 0,
	PROTOTCOL_OSS,
	PROTOTCOL_UNKNOW
}protocolType;

class readOption
{
public:
    readOption()
	{
        batch_size = READ_BATCH_SIZE;
        includes_columns.clear();
		enableCache = false;
		allocBuffer = NULL;
		transactionTable = false;
		blockPolicySize = BLOCK_POLICY_SIZE;
		nPartitionKey = 0;
		ptype = PROTOTCOL_UNKNOW;
    }

    std::vector<bool> includes_columns;
    int batch_size;
	int blockPolicySize;
	bool enableCache;
	char* allocBuffer;
	bool transactionTable;
    dataBufferArray buffer;
	int nPartitionKey;
	protocolType ptype;
};

class writeOption {
public:
    writeOption() {
        compression = UNSUPPORTCOMPRESS;
    }

    writeOption(CompressType compress)
    {
        compression = compress;
    }

    CompressType compression;
};

class RewrLogicalBase
{
public:

};

class readLogical : public RewrLogicalBase
{
public:

	virtual void initParameter(void *sstate);

	virtual void initReadLogical();

	virtual bool readNextGroup();

	virtual bool getNextGroup();

	virtual bool readNextFile();

	virtual bool readNextRow(Datum *values, bool *nulls);

	virtual bool getRow(Datum *values, bool *nulls);

	virtual void closeFile();

	virtual void createPolicy();

	virtual fileState getFileState();

	void extraFragmentLists(std::vector<ListContainer> &lists, List *fragmentLists);

	ossFileStream createFileStream();

	void releaseResources();

	/* sync partition tables */
	void initializeColumnValue();

private:
	void setOptionAttrsUsed(std::set<int> used);

	void getAttrsUsed(List* retrieved_attrs, std::set<int> &attrs_used);

	void setPartitionNulls(bool* nulls);

protected:
	int segId;
	int segnum;
	int tupleIndex;
	dataLakeFdwScanState* scanstate;
	TupleDesc tupdesc;
    AttrNumber ncolumns;
	std::set<int> attrs_used;
	std::string curFileName;
	int blockSerial;
	readOption options;
	ossFileStream fileStream;
	bool last;

	/* for partition table */
	int *defMap;
    ExprState **defExprs;
	int nPartitionKey;
	int nConstraint;
	int nDefaults;
	bool *includes_columns;
};

int setStreamFlag(readOption opt);

bool setStreamWhetherCache(readOption opt);

}
}

#endif