/*
 * extensible_protocol.h
 * 	Definitions for extensible protocol types
 *
 *
 * Copyright (c) 2023, Cloudberry Database, HashData Technology Limited.
 *
 * IDENTIFICATION
 * 	src/interfaces/libpq/extensible_protocol.h
 */
#ifndef FRONTEND

#ifndef PG_EXTENSIBLE_PROTOCOL_H
#define PG_EXTENSIBLE_PROTOCOL_H

#include "libpq-fe.h"

typedef struct ExtensibleProtocol
{
    const char extprotocoltype;		/* identifier of ExtensibleProtocolMethods */
} ExtensibleProtocol;

typedef struct ExtensibleProtocolMethods
{
    const char  extprotocoltype;
    void 	(*protocolSend) (void *tuple);
    int		(*protocolRecv) (PGconn *conn, int msgLength);
} ExtensibleProtocolMethods;

extern void
RegisterExtensibleProtocolMethods(const ExtensibleProtocolMethods *methods);
extern const ExtensibleProtocolMethods *
GetExtensibleProtocolMethods(const char extprotocoltype, bool missing_ok);
extern void
UnegisterExtensibleProtocolMethods(const ExtensibleProtocolMethods *methods);

#endif
#endif