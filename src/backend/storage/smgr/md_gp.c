/*-------------------------------------------------------------------------
 *
 * md_gp.c
 *	  GPDB extensions to md.c
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/storage/smgr/md_gp.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "storage/fd.h"
#include "storage/smgr.h"
#include "cdb/cdbmirroredbufferpool.h"
#include "cdb/cdbpersistenttablespace.h"


/* copied from md.c */
#ifndef WIN32
#define FILE_POSSIBLY_DELETED(err)	((err) == ENOENT)
#else
#define FILE_POSSIBLY_DELETED(err)	((err) == ENOENT || (err) == EACCES)
#endif

int
errdetail_nonexistent_relation(int error, RelFileNode *relFileNode)
{
#define MAX_MSG_LEN MAXPGPATH + 100

	char	   *buffer;
	int			snprintfResult;

	char	   *primaryFilespaceLocation = NULL;
	char	   *mirrorFilespaceLocation = NULL;

	struct stat fst;

	if (!FILE_POSSIBLY_DELETED(error))
	{
		/*
		 * No call to errdetail.
		 */
		return 0;
	}

	buffer = (char *) palloc(MAX_MSG_LEN);

	if (IsBuiltinTablespace(relFileNode->spcNode))
	{
		/*
		 * Assume filespace and tablespace exists.
		 */
	}
	else
	{
		char	   *tablespacePath;

		/*
		 * Investigate whether the containing directories exist to give more
		 * detail.
		 */
		PersistentTablespace_GetPrimaryAndMirrorFilespaces(
														   relFileNode->spcNode,
														   &primaryFilespaceLocation,
														   &mirrorFilespaceLocation);
		if (primaryFilespaceLocation == NULL ||
			strlen(primaryFilespaceLocation) == 0)
		{
			elog(ERROR, "Empty primary filespace directory location");
		}

		if (mirrorFilespaceLocation != NULL)
		{
			pfree(mirrorFilespaceLocation);
			mirrorFilespaceLocation = NULL;
		}

		/*
		 * Does the filespace directory exist?
		 */
		if (stat(primaryFilespaceLocation, &fst) < 0)
		{
			int			saved_err;

			saved_err = errno;

			if (FILE_POSSIBLY_DELETED(saved_err))
			{
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Filespace directory \"%s\" does not exist",
							 primaryFilespaceLocation);
			}
			else
			{
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Filespace directory \"%s\" existence check: %s",
							 primaryFilespaceLocation,
							 strerror(saved_err));
			}

			Assert(snprintfResult >= 0);
			Assert(snprintfResult < MAX_MSG_LEN);

			/*
			 * Give DETAIL on the filespace directory!
			 */
			errdetail("%s", buffer);
			pfree(buffer);

			pfree(primaryFilespaceLocation);
			return 0;
		}
		else
		{
			if (Debug_persistent_recovery_print)
			{
				/*
				 * Since we are being invoked inside an elog, we don't want to
				 * recurse.  So, use errdetail instead!
				 */
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Filespace directory \"%s\" does exist",
							 primaryFilespaceLocation);

				Assert(snprintfResult >= 0);
				Assert(snprintfResult < MAX_MSG_LEN);

				errdetail("%s", buffer);
			}
		}

		tablespacePath = (char *) palloc(MAXPGPATH + 1);

		FormTablespacePath(tablespacePath,
						   primaryFilespaceLocation,
						   relFileNode->spcNode);

		/*
		 * Does the tablespace directory exist?
		 */
		if (stat(tablespacePath, &fst) < 0)
		{
			int			saved_err;

			saved_err = errno;

			if (FILE_POSSIBLY_DELETED(saved_err))
			{
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Tablespace directory \"%s\" does not exist",
							 tablespacePath);
			}
			else
			{
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Tablespace directory \"%s\" existence check: %s",
							 tablespacePath,
							 strerror(saved_err));
			}

			Assert(snprintfResult >= 0);
			Assert(snprintfResult < MAX_MSG_LEN);

			/*
			 * Give DETAIL on the tablespace directory!
			 */
			errdetail("%s", buffer);
			pfree(buffer);

			if (primaryFilespaceLocation != NULL)
				pfree(primaryFilespaceLocation);

			pfree(tablespacePath);
			return 0;
		}
		else
		{
			if (Debug_persistent_recovery_print)
			{
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Tablespace directory \"%s\" does exist",
							 tablespacePath);

				Assert(snprintfResult >= 0);
				Assert(snprintfResult < MAX_MSG_LEN);

				errdetail("%s", buffer);
			}
		}

		pfree(tablespacePath);
	}

	if (relFileNode->spcNode != GLOBALTABLESPACE_OID)
	{
		char	   *dbPath;

		dbPath = (char *) palloc(MAXPGPATH + 1);

		FormDatabasePath(dbPath,
						 primaryFilespaceLocation,
						 relFileNode->spcNode,
						 relFileNode->dbNode);

		/*
		 * Does the database directory exist?
		 */
		if (stat(dbPath, &fst) < 0)
		{
			int			saved_err;

			saved_err = errno;

			if (FILE_POSSIBLY_DELETED(saved_err))
			{
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Database directory \"%s\" does not exist",
							 dbPath);
			}
			else
			{
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Database directory \"%s\" existence check: %s",
							 dbPath, strerror(saved_err));
			}

			Assert(snprintfResult >= 0);
			Assert(snprintfResult < MAX_MSG_LEN);

			/*
			 * Give DETAIL on the database directory!
			 */
			errdetail("%s", buffer);
			pfree(buffer);

			if (primaryFilespaceLocation != NULL)
				pfree(primaryFilespaceLocation);

			pfree(dbPath);
			return 0;
		}
		else
		{
			if (Debug_persistent_recovery_print)
			{
				snprintfResult =
					snprintf(buffer, MAX_MSG_LEN,
							 "Database directory \"%s\" does exist",
							 dbPath);

				Assert(snprintfResult >= 0);
				Assert(snprintfResult < MAX_MSG_LEN);

				errdetail("%s", buffer);
			}
		}

		pfree(dbPath);
	}

	pfree(buffer);
	if (primaryFilespaceLocation != NULL)
		pfree(primaryFilespaceLocation);

	/*
	 * No (normal) call to errdetail.
	 */
	return 0;
}




/*
 *	mdcreatefilespacedir() -- Create a new filespace directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the filespace directory to exist already.
 */
void
mdcreatefilespacedir(
					 Oid filespaceOid,
					 char *primaryFilespaceLocation,

 /*
  * The primary filespace directory path.  NOT Blank padded. Just a NULL
  * terminated string.
  */
					 char *mirrorFilespaceLocation,
					 StorageManagerMirrorMode mirrorMode,
					 bool ignoreAlreadyExists,
					 int *primaryError,
					 bool *mirrorDataLossOccurred)
{
	*primaryError = 0;

	if (mkdir(primaryFilespaceLocation, 0700) < 0)
		*primaryError = errno;
}

/*
 *	mdcreatetablespacedir() -- Create a new tablespace directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the tablespace directory to exist already.
 */
void
mdcreatetablespacedir(Oid tablespaceOid,
					  StorageManagerMirrorMode mirrorMode,
					  bool ignoreAlreadyExists,
					  int *primaryError,
					  bool *mirrorDataLossOccurred)
{
	char	   *primaryFilespaceLocation;
	char	   *mirrorFilespaceLocation;
	char		tablespacePath[MAXPGPATH];

	*primaryError = 0;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(tablespaceOid,
													   &primaryFilespaceLocation,
													   &mirrorFilespaceLocation);

	FormTablespacePath(tablespacePath,
					   primaryFilespaceLocation,
					   tablespaceOid);

	if (mkdir(tablespacePath, 0700) < 0)
		*primaryError = errno;

	if (primaryFilespaceLocation)
		pfree(primaryFilespaceLocation);
	if (mirrorFilespaceLocation)
		pfree(mirrorFilespaceLocation);
}

/*
 *	mdcreatedbdir() -- Create a new database directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the database directory to exist already.
 */
void
mdcreatedbdir(DbDirNode *dbDirNode,
			  StorageManagerMirrorMode mirrorMode,
			  bool ignoreAlreadyExists,
			  int *primaryError,
			  bool *mirrorDataLossOccurred)
{
	char	   *primaryFilespaceLocation;
	char	   *mirrorFilespaceLocation;
	char		dbPath[MAXPGPATH];

	*primaryError = 0;
	*mirrorDataLossOccurred = false;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(dbDirNode->tablespace,
													   &primaryFilespaceLocation,
													   &mirrorFilespaceLocation);

	FormDatabasePath(dbPath,
					 primaryFilespaceLocation,
					 dbDirNode->tablespace,
					 dbDirNode->database);

	if (mkdir(dbPath, 0700) < 0)
	{
		if (ignoreAlreadyExists && (errno == EEXIST))
		{
			elog(LOG, "Directory already exists %s", dbPath);
		}
		else
		{
			*primaryError = errno;
		}
	}

	if (primaryFilespaceLocation)
		pfree(primaryFilespaceLocation);
	if (mirrorFilespaceLocation)
		pfree(mirrorFilespaceLocation);
}

/*
 *	mdrmfilespacedir() -- Remove a filespace directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the filespace directory to exist already.
 */
bool
mdrmfilespacedir(Oid filespaceOid,
				 char *primaryFilespaceLocation,

 /*
  * The primary filespace directory path.  NOT Blank padded. Just a NULL
  * terminated string.
  */
				 char *mirrorFilespaceLocation,
				 bool primaryOnly,
				 bool mirrorOnly,
				 bool ignoreNonExistence,
				 bool *mirrorDataLossOccurred)
{
	/*
	 * The rmtree routine unfortunately emits errors, so there is not
	 * errno available...
	 */
	/* Just a bool. */
	return rmtree(primaryFilespaceLocation, true);
}

/*
 *	mdrmtablespacedir() -- Remove a tablespace directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the tablespace directory to exist already.
 */
bool
mdrmtablespacedir(Oid tablespaceOid,
				  bool primaryOnly,
				  bool mirrorOnly,
				  bool ignoreNonExistence,
				  bool *mirrorDataLossOccurred)
{
	char	   *primaryFilespaceLocation;
	char	   *mirrorFilespaceLocation;
	char		tablespacePath[MAXPGPATH];
	bool		result;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(tablespaceOid,
													   &primaryFilespaceLocation,
													   &mirrorFilespaceLocation);

	/*
	 * We've removed all relations, so all that is left are PG* files and
	 * work files.
	 */
	FormTablespacePath(tablespacePath,
					   primaryFilespaceLocation,
					   tablespaceOid);

	/*
	 * The rmtree routine unfortunately emits errors, so there is not
	 * errno available...
	 */
	/* Just a bool. */
	result = rmtree(tablespacePath, true);

	if (primaryFilespaceLocation != NULL)
		pfree(primaryFilespaceLocation);
	if (mirrorFilespaceLocation != NULL)
		pfree(mirrorFilespaceLocation);

	return result;
}


/*
 *	mdrmbdir() -- Remove a database directory on magnetic disk.
 *
 * If isRedo is true, it's okay for the database directory to exist already.
 */
bool
mdrmdbdir(DbDirNode *dbDirNode,
		  bool primaryOnly,
		  bool mirrorOnly,
		  bool ignoreNonExistence,
		  bool *mirrorDataLossOccurred)
{
	char	   *primaryFilespaceLocation;
	char	   *mirrorFilespaceLocation;
	char		dbPath[MAXPGPATH];
	bool		result;

	*mirrorDataLossOccurred = false;

	PersistentTablespace_GetPrimaryAndMirrorFilespaces(dbDirNode->tablespace,
													   &primaryFilespaceLocation,
													   &mirrorFilespaceLocation);

	/*
	 * We've removed all relations, so all that is left are PG* files and
	 * work files.
	 */
	FormDatabasePath(dbPath,
					 primaryFilespaceLocation,
					 dbDirNode->tablespace,
					 dbDirNode->database);

	/*
	 * The rmtree routine unfortunately emits errors, so there is not
	 * errno available...
	 */
	/* Just a bool. */
	result = rmtree(dbPath, true);

	if (primaryFilespaceLocation != NULL)
		pfree(primaryFilespaceLocation);
	if (mirrorFilespaceLocation != NULL)
		pfree(mirrorFilespaceLocation);

	return result;
}
