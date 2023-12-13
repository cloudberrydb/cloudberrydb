#include "postgres.h"
#include "miscadmin.h"
#include "postmaster/postmaster.h"
#include "dfs_utils.h"

const char *
GetGopherSockertPath(void)
{
	static char path[MAXPGPATH] = {0};

	if (path[0] == '\0')
		snprintf(path, MAXPGPATH, "/tmp/.s.gopher.%d", PostPortNumber);

	return path;
}

const char *
GetGopherPlasmaSocketPath(void)
{
	static char path[MAXPGPATH] = {0};

	if (path[0] == '\0')
		snprintf(path, MAXPGPATH, "/tmp/.s.gopher.plasma.%d", PostPortNumber);

	return path;
}

const char *
GetGopherMetaDataPath(void)
{
	static char path[MAXPGPATH] = {0};

	if (path[0] == '\0')
		snprintf(path, MAXPGPATH, "%s/%s", DataDir, "gophermeta");

	return path;
}

char *
StringTrim(char *value, char c)
{
    char *end;

    while (*value == c)
		value++;

    end = value + strlen(value) - 1;
    while (end > value && *end == c)
		end--;

    *(end + 1) = '\0';

    return value;
}

void
splitPath(const char *path, char **bucket, char **workDir)
{
	char *sep;
	const char *pos = path + 1;

	*workDir = NULL;

	sep = strchr(pos, '/');
	if (sep == NULL)
	{
		*bucket = pstrdup(pos);
		return;
	}

	*bucket = pnstrdup(pos, sep - pos);
	if (*(sep + 1) != '\0')
		*workDir = pstrdup(sep);
}
