#ifndef UFS_CONNECTION_H
#define UFS_CONNECTION_H

#include <gopher/gopher.h>

extern gopherFS UfsGetConnection(const char *serverName, const char *path);

#endif  /* UFS_CONNECTION_H */
