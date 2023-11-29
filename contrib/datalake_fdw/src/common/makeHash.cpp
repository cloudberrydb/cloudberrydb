#include "makeHash.h"

namespace Datalake {
namespace Internal {

CdbHash* inithash(int numsegs)
{
	CdbHash *h;
	Assert(numsegs > 0);
	h = (CdbHash*)palloc(sizeof(CdbHash));
	h->hash = 0;
	h->numsegs = numsegs;
	h->reducealg = REDUCE_JUMP_HASH;
	return h;
}

void resethashkey(CdbHash *h)
{
	h->hash = 0;
}

void makehashkey(CdbHash *h, const char* key)
{
	if (key == NULL)
		return;

	Datum value = hash_any((const unsigned char*)key, strlen(key));
	uint32 hkey = DatumGetUInt32(value);
	h->hash ^= hkey;
}

int reducehash(CdbHash *h)
{
	int			result = 0;
	result = cdbhashreduce(h);
	return result;
}

void freehash(CdbHash *h)
{
	if (h)
	{
		pfree(h);
	}
}

}
}
