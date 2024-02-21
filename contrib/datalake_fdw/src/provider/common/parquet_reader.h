#ifndef PARQUET_READER_H
#define PARQUET_READER_H

#include <memory>
#include <vector>

#include <parquet/api/reader.h>
#include <parquet/io/interface_hdw.h>
#include <gopher/gopher.h>
#include "base_reader.h"

class ParquetReader : public BaseFileReader
{
private:
	int numColumns_;
	std::string filePath_;
	gopherFS gopherFilesystem_;
	std::vector<int> rowGroups_;
	std::unique_ptr<parquet::ParquetFileReader> reader_;
	std::vector<std::shared_ptr<parquet::Scanner>> scanners_;

	bool invalidFileOffset(int64_t startIndex, int64_t preStartIndex, int64_t preCompressedSize);
	void filterRowGroupByOffset(int64_t startOffset, int64_t endOffset);
	void adjustIntegerStringWithScale(int32_t scale, std::string *strDecimal);
	Datum formatDecimal(int64_t value, bool isNull, int scale, int typeMod);

protected:
	Datum readPrimitive(const TypeInfo &typInfo, bool &isNull);
	Datum readDecimal(std::shared_ptr<parquet::Scanner> &scannner, const TypeInfo &typinfo, bool &isNull);
	bool readNextRowGroup();
	void createMapping(List *columnDesc, bool *attrUsed);
	void decodeRecord();

public:
	ParquetReader(MemoryContext rowContext, char *filePath, gopherFS gopherFilesystem);
	~ParquetReader();

	void open(List *columnDesc, bool *attrUsed, int64_t startOffset, int64_t endOffset);
	void close();
};

#endif // PARQUET_READER_H
