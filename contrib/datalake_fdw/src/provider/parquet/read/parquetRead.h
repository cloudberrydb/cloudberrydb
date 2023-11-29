#ifndef DATALAKE_PARQUETREAD_H
#define DATALAKE_PARQUETREAD_H

#include "parquetFileReader.h"
#include "src/common/readPolicy.h"
#include "src/provider/provider.h"


namespace Datalake {
namespace Internal {
class parquetRead : public Provider, public readLogical
{
public:

	virtual void createHandler(void *sstate);

	virtual int64_t read(void *values, void *nulls);

	virtual void destroyHandler();

private:
	virtual void createPolicy();

	virtual bool getNextGroup();

	virtual bool readNextFile();

	bool getRowGropFromSmallFile(metaInfo info);

	bool getRowGropFromBigFile(metaInfo info);

	virtual fileState getFileState();

	virtual bool getRow(Datum *values, bool *nulls);

	bool convertToDatum(Datum *values, bool *nulls);

	void restart();

	readBlockPolicy blockPolicy;
	std::vector<int> rowGroupNums;
    std::vector<int> tempRowGroupNums;
	int curRowGroupNum;
	parquetFileReader fileReader;
};

}
}

#endif