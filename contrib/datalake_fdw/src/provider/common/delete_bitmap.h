#ifndef DELETE_BITMAP_H
#define DELETE_BITMAP_H

#include "roaring.hh"

roaring::Roaring64Map *cppCreateBitmap(void);
void cppDestroyBitmap(roaring::Roaring64Map *roaring);
void cppBitmapAdd(roaring::Roaring64Map *roaring, uint64_t value);
bool cppBitmapContains(roaring::Roaring64Map *roaring, uint64_t value);

#endif // DELETE_BITMAP_H
