/*-------------------------------------------------------------------------
 *
 * tag.h
 *		Tag management commands (create/drop/alter tag).
 *
 *
 * Portions Copyright (c) 2024	Hashdata Inc
 *
 * src/include/commands/tag.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TAG_H
#define TAG_H
#include "catalog/objectaddress.h"

extern Oid CreateTag(CreateTagStmt *stmt);
extern ObjectAddress AlterTag(AlterTagStmt *stmt);
extern void DropTag(DropTagStmt *stmt);
extern void AddTagDescriptions(List *tags, Oid databaseid, Oid classid, Oid objid, char *objname);
extern void AlterTagDescriptions(List *tags, Oid databaseid, Oid classid, Oid objid, char *objname);
extern void UnsetTagDescriptions(List *tags, Oid databaseid, Oid classid, Oid objid, char *objname);
extern void DeleteTagDescriptions(Oid databaseid, Oid classid, Oid objid);
extern Oid get_tag_oid(const char *tagname, bool missing_ok);
extern ObjectAddress RenameTag(const char *oldname, const char *newname);
extern char *TagGetNameByOid(Oid tagid, bool missing_ok);

#endif							/* TAG_H */