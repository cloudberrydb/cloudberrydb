#include <list>
#include <cassert>
#include <sstream>
#include "orcWriter.h"

extern "C" {
#include "utils/date.h"
#include "utils/timestamp.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
}



using namespace orc;

#define GP_VCHAR_MAX_SIZE (10485760)
#define GP_NUMERIC_MAX_SIZE (1000)

/* =======================================================================
 *                            orcWriter
 * =======================================================================
 */

orcWrite::orcWrite() :
  stringDataBuff(*orc::getDefaultPool(), (uint64_t)ORC_DEFAULT_POOL_SIZE),
  intervalDataBuff(*orc::getDefaultPool(), (uint64_t)ORC_DEFAULT_POOL_SIZE),
  timeDataBuff(*orc::getDefaultPool(), (uint64_t)ORC_DEFAULT_POOL_SIZE)
{

}
orcWrite::~orcWrite()
{

}

void orcWrite::createHandler(void* sstate)
{
	fileIndex = 0;
	tupleIndex = 0;
	stripeIndex = 0;
	totalStripes = 0;
	segid = GpIdentity.segindex;
	segnum = getgpsegmentCount();
	batchHasNULL = NULL;
	stringBuffOffset = 0;
	intervalBuffOffset = 0;
	timeBuffOffset = 0;

	fdwState = (dataLakeFdwScanState*)sstate;
	Relation relation = fdwState->rel;
	if (relation == NULL)
	{
		elog(ERROR, "Orc get Relation failed");
	}
	ncolumns = relation->rd_att->natts;
	tupdesc = relation->rd_att;
	batchHasNULL = (bool*)palloc0(sizeof(bool) * ORC_COLUMNS);
	ResetColumnVectorBatchHasNULL();

	gopherConfig* conf = createGopherConfig((void*)(fdwState->options->gopher));
	fileStream = createFileSystem(conf);
	freeGopherConfig(conf);

	initORC();
}

columType orcWrite::columnBelongType(int attColumn)
{
	columType type = columType::OtherType;
	switch (attColumn)
	{
		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
		case BYTEAOID:
		case TEXTOID:
		{
			type = columType::StringColumn;
			break;
		}
		case INTERVALOID:
		{
			type = columType::IntervalColumn;
			break;
		}
		case TIMEOID:
		{
			type = columType::TimeColumn;
			break;
		}
		default:
		{
			break;
		}
	}
	return type;
}

void orcWrite::initORC()
{
	std::string orcName = generateWriteFileName(fdwState->options->prefix, ORC_WRITE_SUFFIX, segid);

	generateOrcSchema();
	batchSize = ORC_BATCH_SIZE;
	uint64_t stripeSize = (64 * 1024 * 1024); // 64M
	uint64_t blockSize = 64 * 1024;     // 64K
	orc::CompressionKind compression = orc::CompressionKind_NONE;
	writeOptions.setStripeSize(stripeSize);
	writeOptions.setCompressionBlockSize(blockSize);
	writeOptions.setCompression(compression);

	outStream = std::unique_ptr<OutputStream>(new OssOutputStream(fdwState->options, orcName, false));
	orcWriter = createWriter(*orcSchema, outStream.get(), writeOptions);
	batch = orcWriter->createRowBatch(batchSize);
	root = dynamic_cast<StructVectorBatch *>(batch.get());
	rows = 0;
}

int64_t orcWrite::write(const void* buf, int64_t length)
{
	//MemoryContext oldcontext = MemoryContextSwitchTo(pstate->rowcontext);
	writeToField(rows, buf);
	rows++;
	totalStripes++;
	if (rows == batchSize) {
		writeToBatch(rows);
		memset(batch->notNull.data(), 1, batchSize);
		root = dynamic_cast<orc::StructVectorBatch*>(batch.get());
		rows = 0;
		stringBuffOffset = 0;
		intervalBuffOffset = 0;
		timeBuffOffset = 0;
	}
	//MemoryContextSwitchTo(oldcontext);
	return length;
}

void orcWrite::SetColumnVectorBatchHasNULL(int column, bool hasNULL)
{
	if (hasNULL)
	{
		batchHasNULL[column] = 1;
	}
}

void orcWrite::ResetColumnVectorBatchHasNULL()
{
	memset(batchHasNULL, 0, sizeof(bool) * ORC_COLUMNS);
}

void orcWrite::fillLongValues(int64_t values,
							orc::ColumnVectorBatch* fields,
							int column,
							bool isNULL,
							int numValues)
{
	orc::LongVectorBatch* longBatch = dynamic_cast<orc::LongVectorBatch*>(fields);
	bool hasNull = false;
	if (isNULL)
	{
		fields->notNull[numValues] = 0;
		hasNull = true;
	}
	else
	{
		fields->notNull[numValues] = 1;
		longBatch->data[numValues] =static_cast<int64_t> (values);
	}
	SetColumnVectorBatchHasNULL(column, hasNull);
}

void orcWrite::fillDoubleValues(double values,
								orc::ColumnVectorBatch* fields,
								int column,
								bool isNULL,
								int numValues)
{
	orc::DoubleVectorBatch* dblBatch = dynamic_cast<orc::DoubleVectorBatch*>(fields);
	bool hasNull = false;
	if (isNULL)
	{
		fields->notNull[numValues] = 0;
		hasNull = true;
	}
	else
	{
		fields->notNull[numValues] = 1;
		dblBatch->data[numValues] = static_cast<double> (values);
	}
  	SetColumnVectorBatchHasNULL(column, hasNull);
}

void orcWrite::fillBoolValues(bool values,
							orc::ColumnVectorBatch* fields,
							int column,
							bool isNULL,
							int numValues)
{
	orc::LongVectorBatch* boolBatch = dynamic_cast<orc::LongVectorBatch*>(fields);
	bool hasNull = false;
	if (isNULL)
	{
		fields->notNull[numValues] = 0;
		hasNull = true;
	}
	else
	{
		fields->notNull[numValues] = 1;
		if (static_cast<int64_t> (values) > 0)
			boolBatch->data[numValues] = true;
		else
			boolBatch->data[numValues] = false;
	}
	SetColumnVectorBatchHasNULL(column, hasNull);
}

// parse fixed point decimal numbers
void orcWrite::fillDecimalValues(std::string numeric,
								orc::ColumnVectorBatch* fields,
								int column,
								bool isNULL,
								int numValues,
								size_t scale,
								size_t precision)
{
	bool hasNull = false;
	if (isNULL)
	{
		fields->notNull[numValues] = 0;
		hasNull = true;
	}
	else
	{
		orc::Decimal128VectorBatch* d128Batch = ORC_NULLPTR;
		orc::Decimal64VectorBatch* d64Batch = ORC_NULLPTR;
		if (precision <= 18)
		{
			d64Batch = dynamic_cast<orc::Decimal64VectorBatch*>(fields);
			d64Batch->scale = static_cast<int32_t>(scale);
		}
		else
		{
			d128Batch = dynamic_cast<orc::Decimal128VectorBatch*>(fields);
			d128Batch->scale = static_cast<int32_t>(scale);
		}

		fields->notNull[numValues] = 1;

		size_t ptPos = numeric.find('.');
		size_t curScale = 0;
		std::string num = numeric;
		if (ptPos != std::string::npos)
		{
			curScale = numeric.length() - ptPos - 1;
			num = numeric.substr(0, ptPos) + numeric.substr(ptPos + 1);
		}
		orc::Int128 decimal(num);
		while (curScale != scale)
		{
			curScale++;
			decimal *= 10;
		}
		if (precision <= 18)
		{
			d64Batch->values[numValues] = decimal.toLong();
		}
		else
		{
			d128Batch->values[numValues] = decimal;
		}
	}
	SetColumnVectorBatchHasNULL(column, hasNull);
}

void orcWrite::resizeDataBuff(int numValues, orc::DataBuffer<char>& buffer, int64_t datalen, int offset, int type) {
	char* oldBufferAddress = buffer.data();
	// Resize the buffer in case buffer does not have remaining space to store the next string.
	while ((int64_t)buffer.size() - offset < datalen)
	{
		buffer.resize(buffer.size() * 2);
	}
	char* newBufferAddress = buffer.data();
	if (newBufferAddress != oldBufferAddress)
	{
		// Refill stringBatch->data with the new addresses, if buffer's address has changed.
		reDistributedDataBuffer(numValues, oldBufferAddress, newBufferAddress, type);
	}
}

void orcWrite::reDistributedDataBuffer(int numValues, char* oldBufferAddress, char* newBufferAddress, int type) {
	for (int index = 0; index < numValues + 1; index++)
	{
		for (int i = 0; i < ncolumns; i++)
		{
			if (columnBelongType(tupdesc->attrs[i].atttypid) == columType(type))
			{
				orc::StringVectorBatch* stringBatch = dynamic_cast<orc::StringVectorBatch*>(root->fields[i]);
				bool notNull = root->fields[i]->notNull[index];
				if (notNull)
				{
					if (stringBatch->data[index] != NULL)
					{
						stringBatch->data[index] = stringBatch->data[index] - oldBufferAddress + newBufferAddress;
					}
				}
			}
		}
	}
}

void orcWrite::fillStringValues(const char* data,
                                orc::ColumnVectorBatch* fields,
                                int column,
                                bool isNULL,
                                int numValues,
                                orc::DataBuffer<char>& buffer,
                                int64_t& offset,
                                int type) {
  orc::StringVectorBatch* stringBatch =
    dynamic_cast<orc::StringVectorBatch*>(fields);
  bool hasNull = false;
  if (isNULL) {
    fields->notNull[numValues] = 0;
    hasNull = true;
  } else {
    if (data == NULL) {
      elog(ERROR, "orc write string buffer is null");
    }
    fields->notNull[numValues] = 1;
    int64_t datalen = static_cast<int64_t> (strlen(data));
    resizeDataBuff(numValues, buffer, datalen, offset, type);

    memcpy(buffer.data() + offset,
           data,
           datalen);
    stringBatch->data[numValues] = buffer.data() + offset;
    stringBatch->length[numValues] = static_cast<int64_t>(datalen);
    offset += datalen;
  }
  SetColumnVectorBatchHasNULL(column, hasNull);
}

// parse date string from format YYYY-mm-dd
void orcWrite::fillDateValues(int32_t date,
                              orc::ColumnVectorBatch* fields,
                              int column,
                              bool isNULL,
                              int numValues) {
  orc::LongVectorBatch* longBatch =
    dynamic_cast<orc::LongVectorBatch*>(fields);
  bool hasNull = false;
  if (isNULL) {
    fields->notNull[numValues] = 0;
    hasNull = true;
  } else {
    fields->notNull[numValues] = 1;
    int32_t days = date + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);
    longBatch->data[numValues] = days;
  }
  SetColumnVectorBatchHasNULL(column, hasNull);
}

// parse timestamp values in seconds
void orcWrite::fillTimestampValues(int64_t timestamp,
                                   orc::ColumnVectorBatch* fields,
                                   int column,
                                   bool isNULL,
                                   int numValues) {

  orc::TimestampVectorBatch* tsBatch =
    dynamic_cast<orc::TimestampVectorBatch*>(fields);
  bool hasNull = false;
  if (isNULL) {
    fields->notNull[numValues] = 0;
    hasNull = true;
  } else {
    if (timestamp < 0) {
      int64_t second = 0;
      int64_t nanosec = (timestamp%1000000) * 1000;
      if (nanosec != 0)
      {
        second = timestamp/1000000 + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *60*60*24 - 1;
        nanosec += 1000000000;
      }
      else
      {
        second = timestamp/1000000 + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *60*60*24;
      }
      tsBatch->data[numValues] = second;
      tsBatch->nanoseconds[numValues] = nanosec;
    } else {
      int64_t second = timestamp/1000000 + (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *60*60*24;
      tsBatch->data[numValues] = second;
      tsBatch->nanoseconds[numValues] = (timestamp%1000000) * 1000;
    }
  }
  SetColumnVectorBatchHasNULL(column, hasNull);
}

std::string orcWrite::getColTypeName(Oid typeOid){
  switch (typeOid) {
    case BOOLOID:           return "boolean<bool>";
    case INT8OID:           return "bigint<int8>";
    case INT4OID:           return "integer<int, int4>";
    case INT2OID:           return "smallint<int2>";
    case FLOAT4OID:         return "real<float4>";
    case FLOAT8OID:         return "double<float8>";
    case DATEOID:           return "date";
    case INTERVALOID:       return "double<float8>";
    case TIMEOID:           return "interval";
    case TIMESTAMPOID:      return "timestamp";
    case NUMERICOID:        return "numeric<decimal>";
    case CHAROID:           return "character<char>";
    case BYTEAOID:          return "bytea";
    case TEXTOID:           return "text";
    case BPCHAROID:         return "character(n)<char(n)>";
    case VARCHAROID:        return "character varying(n)<varchar(n)>";
    default:
      return "OSS protocol not supported data type.";
  }
}

std::string orcWrite::getTypeMappingSupported(){
  return "\n           Type Mapping Supported\n"
    " External Data Type  |  Orc Data Type \n"
    " --------------------------------------------------------------------\n"
    " bool                |  boolean, bigint, int, smallint, tinyint, date\n"
    " int,int2,int4,int8  |  boolean, bigint, int, smallint, tinyint, date\n"
    " date                |  boolean, bigint, int, smallint, tinyint, date\n"
    " float4,float8       |  float,   double\n"
    " interval            |  string,  varchar\n"
    " time                |  string,  varchar\n"
    " timestamp           |  timestamp\n"
    " numeric             |  decimal\n"
    " char(n),varchar(n)  |  binary,  char, varchar, string\n"
    " bytea,text          |  binary,  char, varchar, string\n";
}

void orcWrite::writeToField(int num, const void* data) {
	TupleTableSlot* slot = (TupleTableSlot*)data;

  for (int i = 0; i < ncolumns; i++) {
    Oid typeID = tupdesc->attrs[i].atttypid;
    std::string columnName = tupdesc->attrs[i].attname.data;
	Datum tts_values = slot->tts_values[i];
	bool isNULL = slot->tts_isnull[i];

    switch (tupdesc->attrs[i].atttypid) {
      case BOOLOID: {
        fillBoolValues(DatumGetBool(tts_values), root->fields[i], i, isNULL, num);
        break;
      }
      case INT2OID:
      case INT4OID:
      case INT8OID: {
        fillLongValues(DatumGetInt64(tts_values), root->fields[i], i, isNULL, num);
        break;
      }
      case FLOAT4OID: {
        fillDoubleValues(DatumGetFloat4(tts_values), root->fields[i], i, isNULL, num);
        break;
      }
      case FLOAT8OID: {
        fillDoubleValues(DatumGetFloat8(tts_values), root->fields[i], i, isNULL, num);
        break;
      }
      case DATEOID: {
        fillDateValues(DatumGetDateADT(tts_values), root->fields[i], i, isNULL, num);
        break;
      }
      case TIMESTAMPOID: {
        fillTimestampValues(DatumGetTimestamp(tts_values), root->fields[i], i, isNULL, num);
        break;
      }
      case NUMERICOID: {
        std::string numeric = "";
        int64_t precision = 0;
        int64_t scale = 0;
        if (!isNULL) {
          precision = ((tupdesc->attrs[i].atttypmod - VARHDRSZ) >> 16) & 0xffff;
          scale = (tupdesc->attrs[i].atttypmod - VARHDRSZ) & 0xffff;
          char *ss = DatumGetCString(DirectFunctionCall1(numeric_out, tts_values));
          if (ss != NULL) {
            numeric = ss;
            pfree(ss);
          }
        }
        fillDecimalValues(numeric, root->fields[i], i, isNULL, num, scale, precision);
        break;
      }
      case CHAROID:
      case BPCHAROID:
      case VARCHAROID:
      case BYTEAOID:
      case TEXTOID: {
        char* text = NULL;
        if (!isNULL) {
          text = DatumGetCString(DirectFunctionCall1(textout, tts_values));
        }
        fillStringValues(text, root->fields[i], i, isNULL, num, stringDataBuff, stringBuffOffset, columType::StringColumn);
        if (text != NULL)
          pfree(text);
        break;
      }
      case INTERVALOID: {
        // No corresponding data type in orc
        char* tinterval = NULL;
        if (!isNULL) {
          tinterval = DatumGetCString(DirectFunctionCall1(interval_out, tts_values));
        }
        fillStringValues(tinterval, root->fields[i], i, isNULL, num, intervalDataBuff, intervalBuffOffset, columType::IntervalColumn);
        if (tinterval != NULL)
          pfree(tinterval);
        break;
      }
      case TIMEOID: {
        // No corresponding data type in orc
        char* times = NULL;
        if (!isNULL) {
          times = DatumGetCString(DirectFunctionCall1(time_out, DatumGetTimeADT(tts_values)));
        }
        fillStringValues(times, root->fields[i], i, isNULL, num, timeDataBuff, timeBuffOffset, columType::TimeColumn);
        if (times != NULL)
          pfree(times);
        break;
      }
      default:
        elog(ERROR,
             "Type Mismatch: data in %s is as define %s in external table, but in orc not define %s",
             columnName.c_str(),
             getColTypeName(typeID).data(),
             getTypeMappingSupported().data());
        break;
    }
  }
}

void orcWrite::writeToBatch(int num) {
  for (int i = 0; i < ncolumns; i++) {
    Oid typeID = tupdesc->attrs[i].atttypid;
    std::string columnName = tupdesc->attrs[i].attname.data;
    switch (typeID)
    {
      case BOOLOID:
      case INT2OID:
      case INT4OID:
      case INT8OID: {
        LongVectorBatch* longBatch = (LongVectorBatch*)root->fields[i];
        longBatch->numElements = num;
        longBatch->hasNulls = batchHasNULL[i];
		break;
      }
      case FLOAT4OID:
      case FLOAT8OID: {
        DoubleVectorBatch* doubleBatch = (DoubleVectorBatch*)root->fields[i];
        doubleBatch->numElements = num;
        doubleBatch->hasNulls = batchHasNULL[i];
        root->numElements = num;
        break;
      }
      case DATEOID: {
        LongVectorBatch* dateBatch = (LongVectorBatch*)root->fields[i];
        dateBatch->numElements = num;
        dateBatch->hasNulls = batchHasNULL[i];
        break;
      }
      case TIMESTAMPOID: {
        TimestampVectorBatch* timestampBatch = (TimestampVectorBatch*)root->fields[i];
        timestampBatch->numElements = num;
        timestampBatch->hasNulls = batchHasNULL[i];
        break;
      }
      case NUMERICOID: {
        (root->fields[i])->numElements = num;
        (root->fields[i])->hasNulls = batchHasNULL[i];
        break;
      }
      case CHAROID:
      case BPCHAROID:
      case VARCHAROID:
      case BYTEAOID:
      case TEXTOID: {
        StringVectorBatch* stringBatch = (StringVectorBatch*)root->fields[i];
        stringBatch->numElements = num;
        stringBatch->hasNulls = batchHasNULL[i];
        break;
      }
      case INTERVALOID: {
        // No corresponding data type in orc
        StringVectorBatch *stringBatch = (StringVectorBatch*)root->fields[i];
        stringBatch->numElements = num;
        stringBatch->hasNulls = batchHasNULL[i];
        break;
      }
      case TIMEOID: {
        // No corresponding data type in orc, needs the source to be string
        StringVectorBatch *stringBatch = (StringVectorBatch*)root->fields[i];
        stringBatch->numElements = num;
        stringBatch->hasNulls = batchHasNULL[i];
        break;
      }
      default:
        elog(ERROR,
             "Type Mismatch: data in %s is as define %s in external table, but in orc not define %s",
             columnName.c_str(),
             getColTypeName(typeID).data(),
             getTypeMappingSupported().data());
        break;
    }
  }
  root->numElements = num;
  orcWriter->add(*batch);
  ResetColumnVectorBatchHasNULL();
}

void orcWrite::destroyHandler() {
  if (rows != 0) {
    writeToBatch(rows);
    rows = 0;
  }
  totalStripes = 0;
  // Close ORC
  orcWriter->close();
  orcWriter.reset();
  stripes.clear();
  batch.reset();
  orcSchema.reset();
  root = NULL;
  stringBuffOffset = 0;
  intervalBuffOffset = 0;
  timeBuffOffset = 0;
  segnum = 0;
  segid = 0;
  rows = 0;

  pfree(batchHasNULL);
}

std::string orcWrite::generateOrcSchema()
{
  orcSchema = createStructType();
  for (int i = 0; i < ncolumns; i++) {
    Oid typeID = tupdesc->attrs[i].atttypid;
    std::string columnName = tupdesc->attrs[i].attname.data;
    switch (typeID)
    {
      case BOOLOID:
      case INT2OID:
      case INT4OID:
      case INT8OID: {
        orcSchema->addStructField(columnName, createPrimitiveType(LONG));
        break;
      }
      case FLOAT4OID:
      case FLOAT8OID: {
        orcSchema->addStructField(columnName, createPrimitiveType(DOUBLE));
        break;
      }
      case DATEOID: {
        orcSchema->addStructField(columnName, createPrimitiveType(DATE));
        break;
      }
      case TIMESTAMPOID: {
        orcSchema->addStructField(columnName, createPrimitiveType(orc::TIMESTAMP));
        break;
      }
      case NUMERICOID: {
        int64_t precision = 0;
        int64_t scale = 0;
        if (tupdesc->attrs[i].atttypmod < 0) {
          precision = 38;
          scale = GP_NUMERIC_MAX_SIZE;
          elog(NOTICE, "numeric precision not specified length set type length (38, 1000).");
        } else {
          precision = ((tupdesc->attrs[i].atttypmod - VARHDRSZ) >> 16) & 0xffff;
          if (precision > 38) {
            precision = 38;
            elog(NOTICE, "numeric precision more than 38 is not supported in orc set type length 38.");
          }
          scale = (tupdesc->attrs[i].atttypmod - VARHDRSZ) & 0xffff;
        }
        orcSchema->addStructField(columnName, createDecimalType(precision, scale));
        break;
      }
      case CHAROID: {
        int64_t len = 0;
        if (tupdesc->attrs[i].atttypmod < 0) {
          len = GP_VCHAR_MAX_SIZE;
          elog(NOTICE, "char not specified length set type length 10485760.");
        } else {
          len = (tupdesc->attrs[i].atttypmod - VARHDRSZ);
        }
        Assert(len > 0);
        orcSchema->addStructField(columnName, createCharType(CHAR, len));
        break;
      }
      case BPCHAROID: {
        int64_t len = 0;
        if (tupdesc->attrs[i].atttypmod < 0) {
          len = GP_VCHAR_MAX_SIZE;
          elog(NOTICE, "bpchar not specified length set type length 10485760.");
        } else {
          len = (tupdesc->attrs[i].atttypmod - VARHDRSZ);
        }
        Assert(len > 0);
        orcSchema->addStructField(columnName, createCharType(VARCHAR, len));
        break;
      }
      case VARCHAROID: {
        int64_t len = 0;
        if (tupdesc->attrs[i].atttypmod < 0) {
          len = GP_VCHAR_MAX_SIZE;
          elog(NOTICE, "varchar not specified length set type length 10485760.");
        } else {
          len = (tupdesc->attrs[i].atttypmod - VARHDRSZ);
        }
        Assert(len > 0);
        orcSchema->addStructField(columnName, createCharType(VARCHAR, len));
        break;
      }
      case BYTEAOID: {
        orcSchema->addStructField(columnName, createPrimitiveType(BINARY));
        break;
      }
      case TEXTOID: {
        orcSchema->addStructField(columnName, createPrimitiveType(STRING));
        break;
      }
      case INTERVALOID: {
        // No corresponding data type in orc
        orcSchema->addStructField(columnName, createPrimitiveType(STRING));
        break;
      }
      case TIMEOID: {
        // No corresponding data type in orc
        orcSchema->addStructField(columnName, createPrimitiveType(STRING));
        break;
      }
      default:
        elog(ERROR,
             "Type Mismatch: data in %s is as define %s in external table, but in orc not define %s",
             tupdesc->attrs[i].attname.data,
             getColTypeName(typeID).data(),
             getTypeMappingSupported().data());
        break;
    }
  }
  return orcSchema->toString();
}

/* =======================================================================
 *                            OssOutputStream
 * =======================================================================
 */

OssOutputStream::OssOutputStream(void* options, std::string filename, bool enableCache)
{
	bytesWritten = 0;
	closed = false;
	this->filename = filename;
	dataLakeOptions* opt = (dataLakeOptions*)options;
	gopherConfig *gopherConf = createGopherConfig((void*)(opt->gopher));
    stream = createFileSystem(gopherConf);
	freeGopherConfig(gopherConf);

	openFile(stream, filename.c_str(), O_WRONLY);
}

OssOutputStream::~OssOutputStream() {
  if (!closed)
  {
    closeFile(stream);
    closed = true;
  }
}

void OssOutputStream::write(const void* buf, size_t length) {
  if (closed) {
    elog(ERROR, "ORC Cannot write to closed stream.");
  }
  if (length <= 0) {
    return;
  }
  int64_t bytesWrite = writeFile(stream, (void*)buf, length);
  if (bytesWrite == -1) {
    elog(ERROR, "ORC Bad write of %s", filename.c_str());
  }
  if (bytesWrite != static_cast<int64_t>(length)) {
    elog(ERROR, "ORC Short write of %s", filename.c_str());
  }
  bytesWritten += static_cast<uint64_t>(bytesWrite);
}

void OssOutputStream::close() {
	if (!closed)
	{
		closeFile(stream);
		closed = true;
	}
}
