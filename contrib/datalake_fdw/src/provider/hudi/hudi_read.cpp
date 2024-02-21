#include "hudi_read.h"

namespace Datalake {
namespace Internal {

void hudiRead::createHandler(void *sstate)
{
    initParameter(sstate);
    protocolContext = createContext(scanstate->options);
    protocolImportStart(scanstate, protocolContext, includes_columns);
}

int64_t hudiRead::read(void *values, void *nulls)
{
    protocolContext->record = (InternalRecord *) palloc0(sizeof(InternalRecord));
    protocolContext->record->nulls = (bool *)nulls;
    protocolContext->record->values = (Datum *)values;

    return rowReaderNext(protocolContext->file->reader, protocolContext->record);
}

void hudiRead::destroyHandler()
{
    releaseResources();
    cleanupContext(protocolContext);
}

}
}
