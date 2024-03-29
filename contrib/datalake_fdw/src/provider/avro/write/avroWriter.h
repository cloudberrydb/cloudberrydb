#ifndef DATALAKE_AVRO_WRITER_H
#define DATALAKE_AVRO_WRITER_H

#include <avro/Schema.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <vector>

#include "LogicalSchema.h"

#include "src/common/logicalType.h"
#include "src/common/rewrLogical.h"

extern "C"{
#include "src/datalake_def.h"
}

using Datalake::Internal::AvroLogicalType;
using Datalake::Internal::writeOption;

class avroWriter : public AvroLogicalType
{
public:
    avroWriter(ossFileStream ossStream, const std::string& fileName, void* sstate, writeOption option);

    ~avroWriter();

    int64_t write(const void* buf, size_t length);

    void close();
private:

    void initSchema();

    void initCompression(writeOption option);
private:
    std::unique_ptr<avro::ValidSchema> schema;
    std::unique_ptr<avro::DataFileWriter<avro::GenericDatum>> file_writer;
    std::unique_ptr<avro::GenericDatum> bufDatum;

    TupleDesc tupdesc;
    AttrNumber ncolumns;
    avro::Codec compression;
};

typedef std::unique_ptr<avroWriter> avroWriterPtr;

#endif //DATALAKE_AVRO_WRITER_H