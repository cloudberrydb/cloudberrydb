#ifndef DATALAKE_ORCREAD_H
#define DATALAKE_ORCREAD_H

#include "src/provider/provider.h"
#include "orcFileReader.h"


namespace Datalake {
namespace Internal {

class orcRead : public Provider, public readLogical
{
public:
	virtual void createHandler(void *sstate);

	virtual int64_t read(void *values, void *nulls);

	virtual void destroyHandler();

protected:
	virtual void createPolicy();

	virtual fileState getFileState();

	virtual bool readNextFile();

	bool getStripeFromSmallFile(metaInfo info);

	bool getStripeFromBigFile(metaInfo info);

	virtual bool getRow(Datum *values, bool *nulls);

	virtual bool getNextGroup();

protected:

	void restart();

	orcReadPolicy readPolicy;
	orcFileReader fileReader;
	int64_t tupleIndex;
	int stripeIndex;
	orcReadDeltaFile deltaFile;
};

}
}
#endif
