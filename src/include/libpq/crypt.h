/*-------------------------------------------------------------------------
 *
 * crypt.h
 *	  Interface to libpq/crypt.c
 *
 * Portions Copyright (c) 1996-2010, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * $PostgreSQL: pgsql/src/include/libpq/crypt.h,v 1.39 2010/01/02 16:58:04 momjian Exp $
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_CRYPT_H
#define PG_CRYPT_H

#include "c.h"

#include "libpq/libpq-be.h"
#include "libpq/md5.h"

extern int hashed_passwd_verify(const Port *port, const char *user,
								char *client_pass);
#endif
