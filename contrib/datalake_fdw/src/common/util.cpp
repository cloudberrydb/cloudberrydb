#include "util.h"
#include <sstream>

extern "C" {
#include "src/datalake_def.h"
}

char *strConvertLow(char *str) {
	if (str == NULL)
	{
		return NULL;
	}
	char *orign = str;
	for (; *str != '\0'; str++)
	{
		*str = tolower(*str);
	}
	return orign;
}
