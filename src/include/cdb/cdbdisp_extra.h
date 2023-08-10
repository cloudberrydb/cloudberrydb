#ifndef CDBDISP_EXTRA_H
#define CDBDISP_EXTRA_H

#include "lib/stringinfo.h"

#define EXTRADISPNAME_MAX_LEN	64

typedef char *(*PackFunc) (int *len);
typedef void (*UnpackFunc) (const char *msg, int len);

extern void RegisterExtraDispatch(const char *extraDispName, PackFunc packFunc, UnpackFunc unpackFunc);
extern char *PackExtraMsgs(int *len);
extern void UnPackExtraMsgs(StringInfo strInfo);

#endif   /* CDBDISP_EXTRA_H */
