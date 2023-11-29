#ifndef DATALAKE_ARCHIEVEREAD_H
#define DATALAKE_ARCHIEVEREAD_H

#include "archiveFileRead.h"
#include "src/common/readPolicy.h"
#include "src/provider/provider.h"


namespace Datalake {
namespace Internal {

class archiveRead : public Provider, public readLogical
{
public:

	virtual void createHandler(void *sstate);

	virtual int64_t readWithBuffer(void* buffer, int64_t length);

	virtual void destroyHandler();

private:
	virtual void createPolicy();

	virtual bool readNextFile();

	virtual fileState getFileState();

	readAccordingConsistentHash readPolicy;

	archiveFileRead fileRead;

	int serial;
};



}
}



#endif