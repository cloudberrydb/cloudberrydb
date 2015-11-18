//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2010 Greenplum, Inc.
//
//  @filename:
//  banned_posix_nonreentrant.h
//
//  @doc:
//  Ban certain APIs. Include this file to disallow functions listed in this file.
//
//  @owner:
//  Siva
//
//  @test:
//
//---------------------------------------------------------------------------

#ifndef BANNED_POSIX_NONREENTRANT_H

#define BANNED_POSIX_NONREENTRANT_H

#include "gpos/common/banned_api.h"

#undef drand48
#define drand48 GPOS_BANNED_API(drand48)

#undef asctime
#define asctime GPOS_BANNED_API(asctime)

#undef encrypt
#define encrypt GPOS_BANNED_API(encrypt)

#undef crypt
#define crypt GPOS_BANNED_API(crypt)

#undef fcvt
#define fcvt GPOS_BANNED_API(fcvt)

#undef ctime
#define ctime GPOS_BANNED_API(ctime)

#undef getgrgid
#define getgrgid GPOS_BANNED_API(getgrgid)

#undef ecvt
#define ecvt GPOS_BANNED_API(ecvt)

#undef getgrnam
#define getgrnam GPOS_BANNED_API(getgrnam)

#undef erand48
#define erand48 GPOS_BANNED_API(erand48)

#undef getnetgrent
#define getnetgrent GPOS_BANNED_API(getnetgrent)

#undef fgetgrent
#define fgetgrent GPOS_BANNED_API(fgetgrent)

#undef getpwnam
#define getpwnam GPOS_BANNED_API(getpwnam)

#undef fgetpwent
#define fgetpwent GPOS_BANNED_API(fgetpwent)

#undef getpwuid
#define getpwuid GPOS_BANNED_API(getpwuid)

#undef getdate
#define getdate GPOS_BANNED_API(getdate)

#undef getutline
#define getutline GPOS_BANNED_API(getutline)

#undef getgrent
#define getgrent GPOS_BANNED_API(getgrent)

#undef gmtime
#define gmtime GPOS_BANNED_API(gmtime)

#undef gethostbyaddr
#define gethostbyaddr GPOS_BANNED_API(gethostbyaddr)

#undef initstate
#define initstate GPOS_BANNED_API(initstate)

#undef gethostbyname2
#define gethostbyname2 GPOS_BANNED_API(gethostbyname2)

#undef jrand48
#define jrand48 GPOS_BANNED_API(jrand48)

#undef gethostbyname
#define gethostbyname GPOS_BANNED_API(gethostbyname)

#undef lcong48
#define lcong48 GPOS_BANNED_API(lcong48)

#undef getmntent
#define getmntent GPOS_BANNED_API(getmntent)

#undef lgamma
#define lgamma GPOS_BANNED_API(lgamma)

#undef getpwent
#define getpwent GPOS_BANNED_API(getpwent)

#undef lgammaf
#define lgammaf GPOS_BANNED_API(lgammaf)

#undef getutent
#define getutent GPOS_BANNED_API(getutent)

#undef lgammal
#define lgammal GPOS_BANNED_API(lgammal)

#undef getutid
#define getutid GPOS_BANNED_API(getutid)

#undef lrand48
#define lrand48 GPOS_BANNED_API(lrand48)

#undef hcreate
#define hcreate GPOS_BANNED_API(hcreate)

#undef nrand48
#define nrand48 GPOS_BANNED_API(nrand48)

#undef hdestroy
#define hdestroy GPOS_BANNED_API(hdestroy)

#undef qecvt
#define qecvt GPOS_BANNED_API(qecvt)

#undef hsearch
#define hsearch GPOS_BANNED_API(hsearch)

#undef rand
#define rand GPOS_BANNED_API(rand)

#undef localtime
#define localtime GPOS_BANNED_API(localtime)

#undef random
#define random GPOS_BANNED_API(random)

#undef mrand48
#define mrand48 GPOS_BANNED_API(mrand48)

#undef readdir64
#define readdir64 GPOS_BANNED_API(readdir64)

#undef ptsname
#define ptsname GPOS_BANNED_API(ptsname)

#undef readdir
#define readdir GPOS_BANNED_API(readdir)

#undef qfcvt
#define qfcvt GPOS_BANNED_API(qfcvt)

#undef seed48
#define seed48 GPOS_BANNED_API(seed48)

#undef srand48
#define srand48 GPOS_BANNED_API(srand48)

#undef setkey
#define setkey GPOS_BANNED_API(setkey)

#undef srandom
#define srandom GPOS_BANNED_API(srandom)

#undef setstate
#define setstate GPOS_BANNED_API(setstate)

#undef ttyname
#define ttyname GPOS_BANNED_API(ttyname)

#undef strerror
#define strerror GPOS_BANNED_API(strerror)

#undef strtok
#define strtok GPOS_BANNED_API(strtok)

#undef tmpnam
#define tmpnam GPOS_BANNED_API(tmpnam)

#endif //#ifndef BANNED_POSIX_NONREENTRANT_H

// EOF

