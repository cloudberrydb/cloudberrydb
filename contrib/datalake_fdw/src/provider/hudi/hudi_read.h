#ifndef DATALAKE_HUDIREAD_H
#define DATALAKE_HUDIREAD_H

#include "src/common/rewrLogical.h"
#include "src/common/readPolicy.h"
#include "src/provider/provider.h"
#include "src/dlproxy/datalake.h"

extern "C" {
	#include "src/provider/common/utils.h"
	#include "src/provider/common/row_reader.h"
	#include "hudi_task_reader.h"
}

namespace Datalake {
namespace Internal {

class hudiRead : public Provider, public readLogical
{
public:
	virtual void createHandler(void *sstate);

	virtual int64_t read(void *values, void *nulls);

	virtual void destroyHandler();

private:
	ProtocolContext *protocolContext;
};

}
}

#endif // DATALAKE_HUDIREAD_H
