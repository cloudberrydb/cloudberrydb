#ifndef DFS_TABLESPACE_H
#define DFS_TABLESPACE_H

#include "postgres.h"

#include "nodes/parsenodes.h"

extern Oid	DfsCreateTableSpace(CreateTableSpaceStmt *stmt);
extern void DfsDropTableSpace(DropTableSpaceStmt *stmt);
extern Oid DfsAlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt);

extern const char *GetDfsTablespaceServer(Oid id);
extern const char *GetDfsTablespacePath(Oid id);
extern bool IsDfsTablespaceById(Oid spcId);
extern Oid getDatabaseTablespace(Oid id);
#endif  /* DFS_TABLESPACE_H */
