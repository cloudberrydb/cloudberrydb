#include "postgres.h"
#include "lib/stringinfo.h"
#include "strings_util.h"

static void
moveArrayElem(char *value)
{
	int i;
	int len = strlen(value);

	if (len < 2)
		return;

	for (i = 1; i < len; i++)
		value[i - 1] = value[i];

	value[len - 2] = '\0';
}

static char *
unEscapeString(const char *value, int len, char deli, char escape)
{
	int   i;
	char *result = pnstrdup(value, len);

	for (i = 0; i < len; i++)
	{
		char curChar = result[i];
		char nextChar = result[i + 1];

		if (curChar != escape)
			continue;

		if (nextChar == deli || nextChar == escape)
			moveArrayElem(result + i);
	}

	return result;
}

static char *
escapeString(char *value, char deli, char escape)
{
	int    i;
	int    j = 0;
	int    len = strlen(value);
	char  *result = palloc0(len * 2);

	for (i = 0; i < len; i++)
	{
		char curChar = value[i];

		if (curChar == deli || curChar == escape)
		{
			result[j++] = escape;
			result[j++] = curChar;
			continue;
		}

		result[j++] = curChar;
	}

	return result;
}

char *
joinString(char **values, char deli, char escape)
{
	int i = 0;
	StringInfoData result;

	initStringInfo(&result);

	for (i = 0; values[i] != NULL; i++)
	{
		char *value = escapeString(values[i], deli, escape);

		appendStringInfoString(&result, value);
		pfree(value);

		appendStringInfo(&result, "%c", deli);
	}

	result.data[strlen(result.data) - 1] = '\0';

	return result.data;
}

List *
splitString_(const char *value, char deli, char escape)
{
	int    i;
	int    pos = 0;
	int    len = strlen(value);
	bool   isEscape;
	List  *result = NIL;

	for (i = 0; i < len; i++)
	{
		isEscape = false;
		if (value[i] == escape)
		{
			isEscape = true;
			i++;
		}

		if (!isEscape && value[i] == deli)
		{
			result = lappend(result, unEscapeString(value + pos, i - pos, deli, escape));
			pos = i + 1;
		}

		if (i == len - 1)
			result = lappend(result, unEscapeString(value + pos, len - pos, deli, escape));
	}

	return result;
}
