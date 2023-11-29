#include "archiveWrite.h"

namespace Datalake {
namespace Internal {



void archiveWrite::createHandler(void *sstate)
{
	dataLakeFdwScanState *ss = (dataLakeFdwScanState*)sstate;
	gopherConfig* conf = createGopherConfig((void*)(ss->options->gopher));
	fileStream = createFileSystem(conf);
	freeGopherConfig(conf);
	std::string prefix = ss->options->prefix;
    setOption(getCompressType(ss->options->compress));

	std::string suffix;
	if (option.compression == GZIP)
	{
		suffix = "gz";
	}
	else if (option.compression == ZIP)
	{
		suffix = "zip";
	}
	else
	{
		if (strcmp(ss->options->format, DATALAKE_OPTION_FORMAT_TEXT) == 0)
		{
			suffix = TEXT_WRITE_SUFFIX;
		} else if (strcmp(ss->options->format, DATALAKE_OPTION_FORMAT_CSV) == 0)
		{
			suffix = CSV_WRITE_SUFFIX;
		}
	}

	fileName = generateArchiveWriteFileName(prefix, suffix);

	fileWrite.open(fileStream, fileName, sstate, option);
}

int64_t archiveWrite::write(const void *buf, int64_t length)
{
	return fileWrite.write(buf, length);
}

void archiveWrite::destroyHandler()
{
	fileWrite.close();
}


std::string archiveWrite::generateArchiveWriteFileName(std::string filePath, std::string suffix)
{
	int segid = GpIdentity.segindex;
	std::string exportName = generateWriteFileName(filePath, suffix, segid);
    return exportName;
}

void archiveWrite::setOption(CompressType compressType)
{
	option.compression = compressType;
}

}
}