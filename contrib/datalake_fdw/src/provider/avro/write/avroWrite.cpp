
#include "avroWrite.h"
#include "src/common/fileSystemWrapper.h"

void avroWrite::createHandler(void* sstate)
{
	dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;
    gopherConfig *conf = createGopherConfig((void*)(ss->options->gopher));
    fileStream = createFileSystem(conf);
    freeGopherConfig(conf);
    std::string prefix = ss->options->prefix;
    setOption(getCompressType(ss->options->compress));
    generateAvroFileName(prefix);
    file_writer=std::make_unique<avroWriter>(fileStream, file_name, sstate, option);
}

int64_t avroWrite::write(const void* buf, int64_t length)
{
    int64_t rownum = file_writer->write(buf, length);
    return rownum;
}

std::string& avroWrite::generateAvroFileName(const std::string &filePath)
{
    int segid = GpIdentity.segindex;
    file_name = generateWriteFileName(filePath, AVRO_WRITE_SUFFIX, segid);
    return file_name;
}

void avroWrite::destroyHandler()
{
    file_writer->close();
    destroyFileSystem(fileStream);
    fileStream = NULL;
}

void avroWrite::setOption(CompressType compressType)
{
    option.compression = compressType;
}
