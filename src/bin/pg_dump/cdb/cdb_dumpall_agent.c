/*-------------------------------------------------------------------------
 *
 * cdb_dumpall_agent.c
 *
 * Copyright (c) 2006-2010, Greenplum inc.
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * $PostgreSQL: pgsql/src/bin/pg_dump/pg_dumpall.c,v 1.57.4.1 2005/04/18 23:48:01 tgl Exp $
 *
 *-------------------------------------------------------------------------
 */

#include "postgres_fe.h"

#include <time.h>
#include <unistd.h>

#ifdef ENABLE_NLS
#include <locale.h>
#endif
#ifndef HAVE_STRDUP
#include "strdup.h"
#endif
#include <errno.h>
#include <time.h>

#include "getopt_long.h"

#ifndef HAVE_INT_OPTRESET
int			optreset;
#endif

#include "dumputils.h"
#include "libpq-fe.h"
#include "pg_backup.h"
#include "pqexpbuffer.h"
#include "mb/pg_wchar.h"


/* version string we expect back from pg_dump */
#define PG_VERSIONSTR "pg_dump (PostgreSQL) " PG_VERSION "\n"


static void dumpUsers(PGconn *conn, bool initdbonly);
static void dumpGroups(PGconn *conn);
static void dumpTablespaces(PGconn *conn);
static void dumpCreateDB(PGconn *conn);
static void dumpDatabaseConfig(PGconn *conn, const char *dbname);
static void dumpUserConfig(PGconn *conn, const char *username);
static void makeAlterConfigCommand(const char *arrayitem, const char *type, const char *name);
static void dumpTimestamp(char *msg);

static PGresult *executeQuery(PGconn *conn, const char *query);

extern const char *progname;
extern Archive *g_fout;

bool		output_clean = false;
bool		skip_acls = false;
bool		verbose = false;
static bool ignoreVersion = false;
int			server_version;


















/*
 * dumpTimestamp
 */
static void
dumpTimestamp(char *msg)
{
	char		buf[256];
	time_t		now = time(NULL);

	if (strftime(buf, 256, "%Y-%m-%d %H:%M:%S %Z", localtime(&now)) != 0)
		ahprintf(g_fout, "-- %s %s\n\n", msg, buf);
}
