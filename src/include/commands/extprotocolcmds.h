/*-------------------------------------------------------------------------
 *
 * extprotocolcmds.h
 *	  prototypes for extprotocolcmds.c.
 *
 *-------------------------------------------------------------------------
 */

#ifndef EXTPROTOCOLCMDS_H
#define EXTPROTOCOLCMDS_H

#include "nodes/pg_list.h"

extern void DefineExtProtocol(List *name, List *parameters, bool trusted);
extern void RemoveExtProtocolById(Oid protOid);
extern void AlterExtProtocolOwner(const char *name, Oid newOwnerId);
extern void RenameExtProtocol(const char *oldname, const char *newname);


#endif   /* EXTPROTOCOLCMDS_H */
