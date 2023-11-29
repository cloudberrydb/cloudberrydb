#ifndef HASHDATA_ORCWRITE_H
#define HASHDATA_ORCWRITE_H


#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include "src/provider/provider.h"
#include <vector>

extern "C" {
#include "src/datalake_def.h"
};



#define ORC_BATCH_SIZE (1000)
#define ORC_COLUMNS (65536)
#define ORC_DEFAULT_POOL_SIZE (4 * 1024 * 1024)
/**
 *  orc write OutputStream
 *
 * */

class OssOutputStream : public orc::OutputStream
{
public:
	OssOutputStream(void* options, std::string filename, bool enableCache);

	~OssOutputStream() override;

	uint64_t getLength() const override {
		return bytesWritten;
	};

	uint64_t getNaturalWriteSize() const override {
		return 128 * 1024;
	};

	void write(const void* buf, size_t length) override;

	const std::string& getName() const override {
		return filename;
	};

	void close() override;

private:
	ossFileStream stream;
	std::string filename;
	uint64_t bytesWritten;
	bool closed;
};

/**
 *  orc write
 * */

enum columType
{
	OtherType = 0,
	StringColumn,
	IntervalColumn,
	TimeColumn
};

class orcWrite : public Provider {
public:
	orcWrite();
	virtual ~orcWrite();

	virtual void createHandler(void* sstate);

	virtual int64_t read(void *values, void *nulls) { return 0; };

	virtual int64_t write(const void* buf, int64_t length);

	virtual void destroyHandler();

private:
	std::string generateOrcSchema();

	void initORC();

	void writeToField(int num, const void* data);

	void writeToBatch(int num);

private:

	std::string getColTypeName(Oid typeOid);

	std::string getTypeMappingSupported();

	void SetColumnVectorBatchHasNULL(int column, bool hasNULL);

	void ResetColumnVectorBatchHasNULL();

	void fillLongValues(int64_t values,
						orc::ColumnVectorBatch* fields,
						int column,
						bool isNULL,
						int numValues);

	void fillDoubleValues(double values,
						orc::ColumnVectorBatch* fields,
						int column,
						bool isNULL,
						int numValues);

	void fillBoolValues(bool values,
						orc::ColumnVectorBatch* fields,
						int column,
						bool isNULL,
						int numValues);

	void fillDecimalValues(std::string numeric,
							orc::ColumnVectorBatch* fields,
							int column,
							bool isNULL,
							int numValues,
							size_t scale,
							size_t precision);

	void fillStringValues(const char* data,
						orc::ColumnVectorBatch* fields,
						int column,
						bool isNULL,
						int numValues,
						orc::DataBuffer<char>& buffer,
						int64_t& offset,
						int type);

	void fillDateValues(int32_t data,
						orc::ColumnVectorBatch* fields,
						int column,
						bool isNULL,
						int numValues);

	void fillTimestampValues(int64_t timestamp,
							orc::ColumnVectorBatch* fields,
							int column,
							bool isNULL,
							int numValues);

	void resizeDataBuff(int numValues, orc::DataBuffer<char>& buffer, int64_t datalen, int offset, int type);

	columType columnBelongType(int attColumn);

	void reDistributedDataBuffer(int numValues, char* oldBufferAddress, char* newBufferAddress, int type);

private:
	/* orc outputstream */
	std::vector<ORC_UNIQUE_PTR<orc::StripeInformation>> stripes;
	ORC_UNIQUE_PTR<orc::Type> orcSchema;
	ORC_UNIQUE_PTR<orc::Writer> orcWriter;
	orc::WriterOptions writeOptions;
	orc::StructVectorBatch *root;
	ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch;
	ORC_UNIQUE_PTR<orc::OutputStream> outStream;

private:
	ossFileStream fileStream;


private:
	/* tupledesc info */
	TupleDesc tupdesc;
	AttrNumber ncolumns;
	int fileIndex;
	size_t tupleIndex;
	size_t stripeIndex;
	int totalStripes;
	int segnum;
	int segid;
	dataLakeFdwScanState *fdwState;

private:
	/* orc info */
	int batchSize;
	int rows;

	bool* batchHasNULL;
	/* orc data buffer for string */
	orc::DataBuffer<char> stringDataBuff;
	int64_t stringBuffOffset;
	/* orc data buffer for inerval */
	orc::DataBuffer<char> intervalDataBuff;
	int64_t intervalBuffOffset;
	/* orc data buffer for time */
	orc::DataBuffer<char> timeDataBuff;
	int64_t timeBuffOffset;
};



#endif //HASHDATA_ORCWRITE_H
