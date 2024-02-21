#include <parquet/arrow/util/decimal.h>
#include <parquet/arrow/result.h>
#include "parquet_reader.h"
#include "common.h"
#include "gopher_random_file.h"

extern "C"
{
#include "postgres.h"
#include "access/tupdesc.h"
#include "utils/memutils.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils.h"
}

ParquetReader::ParquetReader(MemoryContext rowContext, char *filePath, gopherFS gopherFilesystem)
	: BaseFileReader(rowContext), numColumns_(0), filePath_(filePath), gopherFilesystem_(gopherFilesystem)
{}

ParquetReader::~ParquetReader()
{}

void
ParquetReader::createMapping(List *columnDesc, bool *attrUsed)
{
	int       i;
	int       j;
	ListCell *lc;
	auto      schema = reader_->metadata()->schema();

	numColumns_ = schema->num_columns();

	foreach_with_count(lc, columnDesc, i)
	{
		FieldDescription *entry = (FieldDescription *) lfirst(lc);
		TypeInfo typInfo = {entry->typeOid, entry->typeMod, InvalidOid, -1};

		typeMap_.push_back(typInfo);

		if (!attrUsed[i])
			continue;

		for (j = 0; j < numColumns_; j++)
		{
			const parquet::ColumnDescriptor *field = schema->Column(j);
			auto fieldName = field->name();

			if (pg_strcasecmp(entry->name, fieldName.c_str()) == 0)
			{
				typeMap_[i].columnIndex_ = j;
				typeMap_[i].fileTypeId_ = mapParquetDataType(field->physical_type());
				break;
			}
		}
	}
}

bool
ParquetReader::readNextRowGroup()
{
	curGroup_++;

	if ((uint) curGroup_ >= rowGroups_.size())
		return false;

	auto rowGroupReader = reader_->RowGroup(rowGroups_[curGroup_]);
	size_t typeSize = typeMap_.size();

	scanners_.clear();
	scanners_.resize(numColumns_);

	for (size_t i = 0; i < typeSize; ++i)
	{
		TypeInfo &typInfo = typeMap_[i];

		if (typInfo.columnIndex_ >= 0)
			scanners_[typInfo.columnIndex_] = parquet::Scanner::Make(rowGroupReader->Column(typInfo.columnIndex_), 1000);
	}

	curRow_ = 0;
	numRows_ = rowGroupReader->metadata()->num_rows();

	return true;
}

void
ParquetReader::open(List *columnDesc, bool *attrUsed, int64 startOffset, int64 endOffset)
{
	std::string filename = convertToGopherPath(filePath_);
	reader_ = parquet::ParquetFileReader::Open(std::make_shared<GopherRandomAccessFile> (gopherFilesystem_, filename));
	createMapping(columnDesc, attrUsed);
	filterRowGroupByOffset(startOffset, endOffset);
}

void
ParquetReader::close()
{
	scanners_.clear();
	reader_->Close();
}

bool
ParquetReader::invalidFileOffset(int64_t startIndex, int64_t preStartIndex, int64_t preCompressedSize)
{
	bool invalid = false;

	// checking the first rowGroup
	if (preStartIndex == 0 && startIndex != 4)
	{
		invalid = true;
		return invalid;
	}

	//calculate start index for other blocks
	int64_t minStartIndex = preStartIndex + preCompressedSize;
	if (startIndex < minStartIndex)
		invalid = true;

	return invalid;
}

void
ParquetReader::filterRowGroupByOffset(int64_t startOffset, int64_t endOffset)
{
	int64_t preStartIndex = 0;
	int64_t preCompressedSize = 0;
	int64_t curRowCount = 0;
	auto metadata = reader_->metadata();

	for (int i = 0; i < metadata->num_row_groups(); i++)
	{
		int64_t totalSize = 0;
		int64_t startIndex;

		if (startOffset == -1)
		{
			rowGroups_.push_back(i);
			rowPositions_.push_back(0);
			continue;
		}

		auto rowGroup = metadata->RowGroup(i);
		startIndex = rowGroup->file_offset();

		if (invalidFileOffset(startIndex, preStartIndex, preCompressedSize))
		{
			if (preStartIndex == 0)
				startIndex = 4;
			else
				startIndex = preStartIndex + preCompressedSize;
		}

		preStartIndex = startIndex;
		preCompressedSize = rowGroup->total_compressed_size();

		for (int j = 0; j < rowGroup->num_columns(); j++)
		{
			auto col = rowGroup->ColumnChunk(j);
			totalSize += col->total_compressed_size();
		}

		int64_t midPoint = startIndex + totalSize / 2;
		if (midPoint >= startOffset && midPoint < endOffset)
		{
			rowGroups_.push_back(i);
			rowPositions_.push_back(curRowCount);
		}

		curRowCount += rowGroup->num_rows();
	}
}

Datum
ParquetReader::readPrimitive(const TypeInfo &typInfo, bool &isNull)
{
	ReaderValue d;
	auto scanner = scanners_[typInfo.columnIndex_];

	switch (typInfo.pgTypeId_)
	{
		case BOOLOID:
		{
			((parquet::TypedScanner<parquet::BooleanType> *)scanner.get())->NextValue(&d.boolValue, &isNull);
			return BoolGetDatum(d.boolValue);
		}
		case INT4OID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return Int32GetDatum(d.int32Value);
		}
		case TIMEOID:
		case INT8OID:
		{
			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			return Int64GetDatum(d.int64Value);
		}
		case FLOAT4OID:
		{
			((parquet::TypedScanner<parquet::FloatType> *)scanner.get())->NextValue(&d.floatValue, &isNull);
			return Float4GetDatum(d.floatValue);
		}
		case FLOAT8OID:
		{
			((parquet::TypedScanner<parquet::DoubleType> *)scanner.get())->NextValue(&d.doubleValue, &isNull);
			return Float8GetDatum(d.doubleValue);
		}
		case BYTEAOID:
		case TEXTOID:
		{
			parquet::ByteArray value;
			((parquet::TypedScanner<parquet::ByteArrayType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			bytea *result = (bytea *) gpdbPalloc(value.len + VARHDRSZ);
			SET_VARSIZE(result, value.len + VARHDRSZ);
			memcpy(VARDATA(result), value.ptr, value.len);
			return PointerGetDatum(result);
		}
		case UUIDOID:
		{
			parquet::ByteArray value;
			((parquet::TypedScanner<parquet::ByteArrayType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			unsigned char *result = (unsigned char *) gpdbPalloc(value.len);
			memcpy(result, value.ptr, 16);
			return PointerGetDatum(result);
		}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			return TimestampGetDatum(time_t_to_timestamptz(d.int64Value / 1000000)); // micorsec, refer to iceberg spec
		}
		case DATEOID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return DateADTGetDatum(d.int32Value + (UNIX_EPOCH_JDATE - POSTGRES_EPOCH_JDATE));
		}
		case NUMERICOID:
			return readDecimal(scanner, typInfo, isNull);

		default:
			throw Error("unsupported column type oid: \"%u\"", typInfo.pgTypeId_);
	}

	PG_RETURN_DATUM(0);
}

void
ParquetReader::adjustIntegerStringWithScale(int32_t scale, std::string *strDecimal)
{
	const bool isNegative = strDecimal->front() == '-';
	const auto isNegativeOffset = static_cast<int32_t>(isNegative);
	const auto len = static_cast<int32_t>(strDecimal->size());
	const int32_t numDigits = len - isNegativeOffset;
	const int32_t adjustedExponent = numDigits - 1 - scale;

	if (scale == 0)
		return;

	// Note that the -6 is taken from the Java BigDecimal documentation.
	if (scale < 0 || adjustedExponent < -6)
	{
		char buff[32];
		// Example 1:
		// Precondition: *strDecimal = "123", isNegativeOffset = 0, numDigits = 3, scale = -2,
		//               adjustedExponent = 4
		// Example 2:
		// Precondition: *strDecimal = "-123", isNegativeOffset = 1, numDigits = 3, scale = 9,
		//               adjustedExponent = -7
		// After inserting decimal point: *strDecimal = "-1.23"
		// After appending exponent: *strDecimal = "-1.23E-7"
		strDecimal->insert(strDecimal->begin() + 1 + isNegativeOffset, '.');
		strDecimal->push_back('E');
		if (adjustedExponent >= 0)
			strDecimal->push_back('+');

		snprintf(buff, sizeof(buff), "%d", adjustedExponent);

		strDecimal->append(buff);
		return;
	}

	if (numDigits > scale)
	{
		const auto n = static_cast<size_t>(len - scale);
		// Example 1:
		// Precondition: *strDecimal = "123", len = numDigits = 3, scale = 1, n = 2
		// After inserting decimal point: *strDecimal = "12.3"
		// Example 2:
		// Precondition: *strDecimal = "-123", len = 4, numDigits = 3, scale = 1, n = 3
		// After inserting decimal point: *strDecimal = "-12.3"
		strDecimal->insert(strDecimal->begin() + n, '.');
		return;
	}

	// Example 1:
	// Precondition: *strDecimal = "123", isNegativeOffset = 0, numDigits = 3, scale = 4
	// After insert: *strDecimal = "000123"
	// After setting decimal point: *strDecimal = "0.0123"
	// Example 2:
	// Precondition: *strDecimal = "-123", isNegativeOffset = 1, numDigits = 3, scale = 4
	// After insert: *strDecimal = "-000123"
	// After setting decimal point: *strdecimal = "-0.0123"
	strDecimal->insert(isNegativeOffset, scale - numDigits + 2, '0');
	strDecimal->at(isNegativeOffset + 1) = '.';
}

Datum
ParquetReader::formatDecimal(int64_t value, bool isNull, int scale, int typeMod)
{
	std::string strInteger = std::to_string(value);
	std::string strDecimal(strInteger);

	if (isNull)
		PG_RETURN_DATUM(0);

	adjustIntegerStringWithScale(scale, &strDecimal);
	return gpdbDirectFunctionCall3(numeric_in,
								   CStringGetDatum(strDecimal.c_str()),
								   ObjectIdGetDatum(InvalidOid),
								   Int32GetDatum(typeMod));
}

Datum
ParquetReader::readDecimal(std::shared_ptr<parquet::Scanner> &scanner, const TypeInfo &typInfo, bool &isNull)
{
	ReaderValue d;
	parquet::FixedLenByteArray value;
	const parquet::ColumnDescriptor *columnDesc = reader_->metadata()->schema()->Column(typInfo.columnIndex_);
	int scale = columnDesc->type_scale();

	// iceberg only support int4, int8, fixed-len
	switch (typInfo.fileTypeId_)
	{
		case INT4OID:
		{
			((parquet::TypedScanner<parquet::Int32Type> *)scanner.get())->NextValue(&d.int32Value, &isNull);
			return formatDecimal(d.int32Value, isNull, scale, typInfo.typeMod_);
		}
		case INT8OID:
		{
			((parquet::TypedScanner<parquet::Int64Type> *)scanner.get())->NextValue(&d.int64Value, &isNull);
			return formatDecimal(d.int64Value, isNull, scale, typInfo.typeMod_);
		}
		default:
		{
			int typeLen = columnDesc->type_length();
			((parquet::TypedScanner<parquet::FLBAType> *)scanner.get())->NextValue(&value, &isNull);
			if (isNull)
				PG_RETURN_DATUM(0);

			parquet_arrow::Result<parquet_arrow::Decimal128> result = parquet_arrow::Decimal128::
																		FromBigEndian(value.ptr, typeLen);
			if (!result.ok())
				throw Error("parquet decimal128: out of range");

			parquet_arrow::Decimal128 decimal = result.ValueOrDie();
			return gpdbDirectFunctionCall3(numeric_in,
										   CStringGetDatum(decimal.ToString(scale).c_str()),
										   ObjectIdGetDatum(InvalidOid),
										   Int32GetDatum(typInfo.typeMod_));
		}
	}

	PG_RETURN_DATUM(0);
}

void ParquetReader::decodeRecord() {}
