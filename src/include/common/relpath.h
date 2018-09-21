/*-------------------------------------------------------------------------
 *
 * relpath.h
 *		Declarations for relpath() and friends
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/common/relpath.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef RELPATH_H
#define RELPATH_H

/*
 *	'pgrminclude ignore' needed here because CppAsString2() does not throw
 *	an error if the symbol is not defined.
 */
#include "catalog/catversion.h" /* pgrminclude ignore */
#include "storage/relfilenode.h"


#define OIDCHARS		10		/* max chars printed by %u */
/*
 * In PostgreSQL, this is called just TABLESPACE_VERSION_DIRECTORY. But in 
 * GPDB, you should use tablespace_version_directory() function instead.
 * This constant has been renamed so that we catch and know to modify all
 * upstream uses of TABLESPACE_VERSION_DIRECTORY.
 */
#define GP_TABLESPACE_VERSION_DIRECTORY	"GPDB_" GP_MAJORVERSION "_" \
									CppAsString2(CATALOG_VERSION_NO)

extern const char *forkNames[];
extern int	forkname_chars(const char *str, ForkNumber *fork);
extern char *relpathbackend(RelFileNode rnode, BackendId backend,
			   ForkNumber forknum);

extern void reldir_and_filename(RelFileNode rnode, BackendId backend, ForkNumber forknum,
					char **dir, char **filename);
extern char *aorelpathbackend(RelFileNode node, BackendId backend, int32 segno);

/* First argument is a RelFileNodeBackend */
#define relpath(rnode, forknum) \
		relpathbackend((rnode).node, (rnode).backend, (forknum))

/* First argument is a RelFileNode */
#define relpathperm(rnode, forknum) \
		relpathbackend((rnode), InvalidBackendId, (forknum))

#define aorelpath(rnode, segno) \
		aorelpathbackend((rnode).node, (rnode).backend, (segno))

#endif   /* RELPATH_H */
