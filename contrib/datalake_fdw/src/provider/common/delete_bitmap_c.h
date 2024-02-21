#ifndef DELETE_BITMAP_C_H
#define DELETE_BITMAP_C_H

#ifdef __cplusplus
extern "C" {
#endif

#include "postgres.h"

void *createBitmap(void);
void bitmapAdd(void *bitmap, uint64 value);
void destroyBitmap(void *bitmap);
bool bitmapContains(void *bitmap, uint64 value);

#ifdef __cplusplus
}
#endif

#endif // DELETE_BITMAP_C_H
