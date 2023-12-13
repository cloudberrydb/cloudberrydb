#ifndef DFS_TABLESPACE_H
#define DFS_TABLESPACE_H

#include "postgres.h"

#include "nodes/parsenodes.h"

typedef struct DfsTableSpaceOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
	bool		stage;               /* is staged dfs tablespace */
	int			serverOffset; /* the server used by the dfs tablespace */
	int			prefixOffset; /* the prefix of the dfs tablespace */
} DfsTableSpaceOptions;

extern Oid	DfsCreateTableSpace(CreateTableSpaceStmt *stmt);
extern void DfsDropTableSpace(DropTableSpaceStmt *stmt);
extern Oid DfsAlterTableSpaceOptions(AlterTableSpaceOptionsStmt *stmt);

extern const char *GetDfsTablespaceServer(Oid id);
extern const char *GetDfsTablespacePath(Oid id);
extern bool IsDfsTablespace(Oid id);
extern void dfsInitializeReloptions(void);

#endif  /* DFS_TABLESPACE_H */
