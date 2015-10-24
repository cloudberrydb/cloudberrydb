#ifndef __ORAFUNC__
#define __ORAFUNC__

#include "postgres.h"
#include "catalog/catversion.h"
#include "nodes/pg_list.h"
#include <sys/time.h>
#include "utils/datetime.h"
#include "utils/datum.h"

#ifndef PG_VERSION_NUM
#define PG_VERSION_NUM		80100	/* assume 8.1. */
#endif

#define TextPCopy(t) \
	DatumGetTextP(datumCopy(PointerGetDatum(t), false, -1))

#define PG_GETARG_IF_EXISTS(n, type, defval) \
	((PG_NARGS() > (n) && !PG_ARGISNULL(n)) ? PG_GETARG_##type(n) : (defval))

/* alignment of this struct must fit for all types */
typedef union vardata
{
	char	c;
	short	s;
	int		i;
	long	l;
	float	f;
	double	d;
	void   *p;
} vardata;

int ora_instr(text *txt, text *pattern, int start, int nth);
int ora_mb_strlen(text *str, char **sizes, int **positions);
int ora_mb_strlen1(text *str);

/*
 * Version compatibility
 */

#if PG_VERSION_NUM >= 80400
extern Oid	equality_oper_funcid(Oid argtype);
#endif

#ifdef GP_VERSION_NUM
#ifndef CStringGetTextDatum
#define CStringGetTextDatum(s) PointerGetDatum(cstring_to_text(s))
#endif
char *
text_to_cstring(const text *t);
text *cstring_to_text(const char *c);
text *cstring_to_text_with_len(const char *c, int n);
#else
#if PG_VERSION_NUM < 80400
#define CStringGetTextDatum(c) \
        DirectFunctionCall1(textin, CStringGetDatum(c))
#define text_to_cstring(t) \
        DatumGetCString(DirectFunctionCall1(textout, PointerGetDatum(t)))
#define cstring_to_text(c) \
        DatumGetTextP(CStringGetTextDatum(c))
text *cstring_to_text_with_len(const char *c, int n);
#endif
#endif

#ifdef GP_VERSION_NUM
#define DatumGetTextPP(X)			((text *) PG_DETOAST_DATUM_PACKED(X))
#define SET_VARSIZE(PTR, len)				SET_VARSIZE_4B  ((varattrib_4b*)(PTR), (len))
#define PG_GETARG_TEXT_PP(n)		DatumGetTextPP(PG_GETARG_DATUM(n))
#define VARDATA_ANY(PTR) \
	 (VARATT_IS_1B(PTR) ? VARDATA_1B(PTR) : VARDATA_4B(PTR))
#define VARSIZE_ANY_EXHDR(PTR) \
	(VARATT_IS_1B_E(PTR) ? VARSIZE_1B_E(PTR)-VARHDRSZ_EXTERNAL : \
	 (VARATT_IS_1B(PTR) ? VARSIZE_1B(PTR)-VARHDRSZ_SHORT : \
	  VARSIZE_4B(PTR)-VARHDRSZ))
#define att_align_nominal(cur_offset, attalign) \
( \
	((attalign) == 'i') ? INTALIGN(cur_offset) : \
	 (((attalign) == 'c') ? (intptr_t) (cur_offset) : \
	  (((attalign) == 'd') ? DOUBLEALIGN(cur_offset) : \
	   ( \
			AssertMacro((attalign) == 's'), \
			SHORTALIGN(cur_offset) \
	   ))) \
)
#define att_addlength_pointer(cur_offset, attlen, attptr) \
( \
	((attlen) > 0) ? \
	( \
		(cur_offset) + (attlen) \
	) \
	: (((attlen) == -1) ? \
	( \
		(cur_offset) + VARSIZE_ANY(attptr) \
	) \
	: \
	( \
		AssertMacro((attlen) == -2), \
		(cur_offset) + (strlen((char *) (attptr)) + 1) \
	)) \
)

List *
stringToQualifiedNameList(const char *string, const char *caller);

#else
#if PG_VERSION_NUM < 80300
#define PGDLLIMPORT				DLLIMPORT
#define session_timezone		global_timezone
#define DatumGetTextPP(p)		DatumGetTextP(p)
#define SET_VARSIZE(PTR, len)	(VARATT_SIZEP((PTR)) = (len))
#define PG_GETARG_TEXT_PP(n)	PG_GETARG_TEXT_P((n))
#define VARDATA_ANY(PTR)		VARDATA((PTR))
#define VARSIZE_ANY_EXHDR(PTR)	(VARSIZE((PTR)) - VARHDRSZ)
#define att_align_nominal(cur_offset, attalign) \
	att_align((cur_offset), (attalign))
#define att_addlength_pointer(cur_offset, attlen, attptr) \
	att_addlength((cur_offset), (attlen), (attptr))
#define stringToQualifiedNameList(string) \
	stringToQualifiedNameList((string), "")
typedef void *SPIPlanPtr;
#endif
#endif

#if PG_VERSION_NUM < 80200
#define ARR_NULLBITMAP(a)		(NULL)
#endif

#endif
