#ifndef DATALAKE_AVRO_WRITE_H
#define DATALAKE_AVRO_WRITE_H

#include "avroWriter.h"
#include "src/provider/provider.h"

class avroWrite : public Provider {

public:
    virtual void createHandler(void* sstate);

	virtual int64_t write(const void* buf, int64_t length);

    virtual void destroyHandler();

private:
    void setOption(CompressType compressType);

private:
    std::string &generateAvroFileName(const std::string &filePath);
    std::string file_name;
    avroWriterPtr file_writer;
    ossFileStream fileStream;
    writeOption option;
};


#endif //DATALAKE_AVRO_WRITE_H