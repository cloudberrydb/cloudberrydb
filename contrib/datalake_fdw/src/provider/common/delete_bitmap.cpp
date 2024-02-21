#include "delete_bitmap.h"
#include "roaring.c"

roaring::Roaring64Map *
cppCreateBitmap(void)
{
	return new roaring::Roaring64Map();
}

void
cppDestroyBitmap(roaring::Roaring64Map *roaring)
{
	delete roaring;
}

void
cppBitmapAdd(roaring::Roaring64Map *roaring, uint64_t value)
{
	roaring->add(value);
}

bool
cppBitmapContains(roaring::Roaring64Map *roaring, uint64_t value)
{
	return roaring->contains(value);
}
