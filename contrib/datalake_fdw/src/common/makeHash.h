#ifndef DATALAKE_MAKEHASH
#define DATALAKE_MAKEHASH

#include <stdio.h>

extern "C"
{
#include "cdb/cdbhash.h"
#include "common/hashfn.h"
#include "utils/fmgrprotos.h"
}

namespace Datalake {
namespace Internal {

CdbHash* inithash(int numsegs);

void resethashkey(CdbHash *h);

void makehashkey(CdbHash *h, const char* key);

int reducehash(CdbHash *h);

void freehash(CdbHash *h);

}
}

#endif