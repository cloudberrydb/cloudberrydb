#ifndef HDATALAKE_PARQUETWRITE_H
#define DATALAKE__PARQUETWRITE_H


#include "parquetFileWriter.h"

class parquetWrite : public Provider {

public:
    void createHandler(void *sstate);

    int64_t read(void *values, void *nulls) { return 0; };

    int64_t write(const void *buf, int64_t length);

    void destroyHandler();

private:
    void setOption(CompressType compressType);

private:
    std::string generateParquetFileName(std::string filePath);
    parquetFileWriter file_writer;
    ossFileStream fileStream;
    writeOption option;
};

#endif
