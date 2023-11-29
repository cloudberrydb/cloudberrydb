#ifndef DATALAKE_OPTION_H
#define DATALAKE_OPTION_H

#include "datalake_def.h"

dataLakeOptions *getOptions(Oid foreigntableid);

List* getCopyOptions(Oid foreigntableid);

void freeDataLakeOptions(dataLakeOptions *options);

void checkValidRecordBatchOpt(dataLakeOptions *options);
#endif							/* DATALAKE_OPTION_H */