#ifndef STRINGS_UTIL_H
#define STRINGS_UTIL_H

#include "nodes/pg_list.h"

extern char *joinString(char **values, char deli, char escape);
extern List *splitString_(const char *value, char deli, char escape);

#endif // STRINGS_UTIL_H
