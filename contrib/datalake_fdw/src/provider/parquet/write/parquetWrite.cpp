#include "parquetWrite.h"
#include "src/common/util.h"
#include "src/common/fileSystemWrapper.h"


#define DATALAKE_EXPORT_NAME ("datalake")


void parquetWrite::createHandler(void *sstate)
{
    /* parquet init */ 
	dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;
	gopherConfig* conf = createGopherConfig((void*)(ss->options->gopher));
	fileStream = createFileSystem(conf);
	freeGopherConfig(conf);
	std::string prefix = ss->options->prefix;
    setOption(getCompressType(ss->options->compress));
    file_writer.createParquetWriter(fileStream, generateParquetFileName(prefix), sstate, option);
}

void parquetWrite::setOption(CompressType compressType)
{
    option.compression = compressType;
}

std::string parquetWrite::generateParquetFileName(std::string filePath)
{
	int segid = GpIdentity.segindex;
	std::string exportName = generateWriteFileName(filePath, PARQUET_WRITE_SUFFIX, segid);
    return exportName;
}

int64_t parquetWrite::write(const void *buf, int64_t length)
{
    // CopyState pstate = (CopyState)EXTPROTOCOL_GET_PSTATE(fcinfo);
    // MemoryContext oldcontext = MemoryContextSwitchTo(pstate->rowcontext);
    int64_t rownum = file_writer.write(buf, length);
    //MemoryContextSwitchTo(oldcontext);
    return rownum;
}

void parquetWrite::destroyHandler()
{
    file_writer.closeParquetWriter();
    destroyFileSystem(fileStream);
    fileStream = NULL;
}