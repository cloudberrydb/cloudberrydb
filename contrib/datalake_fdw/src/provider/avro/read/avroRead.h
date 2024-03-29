#ifndef DATALAKE_EXT_AVRO_READ_H
#define DATALAKE_EXT_AVRO_READ_H

#include <avro/GenericDatum.hh>

#include "avroBlockReader.h"
#include "src/common/logicalType.h"
#include "src/common/rewrLogical.h"

namespace Datalake {
namespace Internal {

class AvroStreamBuffer
{
public:
    uint8_t *buf;
    size_t size_;
    AvroStreamBuffer(size_t size) : size_(size)
    {
        buf = (uint8_t*)palloc0(size);
    }
    ~AvroStreamBuffer()
    {
        pfree(buf);
        buf = nullptr;
    }
};

class avroRead : public Provider, public readLogical, public AvroLogicalType
{
public:

    virtual void createHandler(void *sstate);

    virtual int64_t read(void *values, void *nulls);

    virtual void destroyHandler();

private:
    bool checkSchema(const avro::ValidSchema &schema);

    virtual void createPolicy();

    virtual bool getNextGroup();

    virtual bool readNextFile();

    bool createBlockReader();

    void restart();

    bool getRowGropFromSmallFile(metaInfo info);

    bool getRowGropFromBigFile(metaInfo info);

    virtual fileState getFileState();

    virtual bool getRow(Datum *values, bool *nulls);

    bool convertToDatum(Datum *values, bool *nulls);

    bool getNextColumn(Datum &values, avro::GenericRecord &record, int idx, bool &isNull);

    readBlockPolicy blockPolicy;
    std::vector<int> rowGroupNums;
    std::vector<int> tempRowGroupNums;
    int curRowGroupNum;

    std::unique_ptr<AvroStreamBuffer> readBuffer_;
    std::unique_ptr<avro::GenericDatum> bufDatum_;
    std::vector<int8_t> nullBranches;
    avroBlockReaderPtr fileReader;
    fileState state_;
};

}
}
#endif //DATALAKE_EXT_AVRO_READ_H