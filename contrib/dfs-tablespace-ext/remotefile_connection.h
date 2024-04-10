#ifndef REMOTEFILE_CONNECTION_H
#define REMOTEFILE_CONNECTION_H

#include <gopher/gopher.h>

#define MAX_DFS_PATH_SIZE	256

extern gopherFS RemoteFileGetConnection(const char *serverName, const char *path);

#endif  /* REMOTEFILE_CONNECTION_H */
