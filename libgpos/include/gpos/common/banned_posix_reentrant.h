//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2010 Greenplum, Inc.
//
//  @filename:
//  banned_posix_reentrant.h
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

#ifndef BANNED_POSIX_REENTRANT_H

#define BANNED_POSIX_REENTRANT_H

#include "gpos/common/banned_api.h"

#ifndef ALLOW___fpending
#undef __fpending
#define __fpending GPOS_BANNED_API(__fpending)
#endif

#ifndef ALLOW___fbufsize
#undef __fbufsize
#define __fbufsize GPOS_BANNED_API(__fbufsize)
#endif

#ifndef ALLOW___fpurge
#undef __fpurge
#define __fpurge GPOS_BANNED_API(__fpurge)
#endif

#ifndef ALLOW___flbf
#undef __flbf
#define __flbf GPOS_BANNED_API(__flbf)
#endif

#ifndef ALLOW___fsetlocking
#undef __fsetlocking
#define __fsetlocking GPOS_BANNED_API(__fsetlocking)
#endif

#ifndef ALLOW___freadable
#undef __freadable
#define __freadable GPOS_BANNED_API(__freadable)
#endif

#ifndef ALLOW___va_copy
#undef __va_copy
#define __va_copy GPOS_BANNED_API(__va_copy)
#endif

#ifndef ALLOW___freading
#undef __freading
#define __freading GPOS_BANNED_API(__freading)
#endif

#ifndef ALLOW__Exit
#undef _Exit
#define _Exit GPOS_BANNED_API(_Exit)
#endif

#ifndef ALLOW___fwritable
#undef __fwritable
#define __fwritable GPOS_BANNED_API(__fwritable)
#endif

#ifndef ALLOW__exit
#undef _exit
#define _exit GPOS_BANNED_API(_exit)
#endif

#ifndef ALLOW___fwriting
#undef __fwriting
#define __fwriting GPOS_BANNED_API(__fwriting)
#endif

#ifndef ALLOW__flushlbf
#undef _flushlbf
#define _flushlbf GPOS_BANNED_API(_flushlbf)
#endif

#ifndef ALLOW__toupper
#undef _toupper
#define _toupper GPOS_BANNED_API(_toupper)
#endif

#ifndef ALLOW__tolower
#undef _tolower
#define _tolower GPOS_BANNED_API(_tolower)
#endif

#ifndef ALLOW_abort
#undef abort
#define abort GPOS_BANNED_API(abort)
#endif

#ifndef ALLOW_a64l
#undef a64l
#define a64l GPOS_BANNED_API(a64l)
#endif

#ifndef ALLOW_abs
#undef abs
#define abs GPOS_BANNED_API(abs)
#endif

#ifndef ALLOW_addmntent
#undef addmntent
#define addmntent GPOS_BANNED_API(addmntent)
#endif

#ifndef ALLOW_accept
#undef accept
#define accept GPOS_BANNED_API(accept)
#endif

#ifndef ALLOW_aio_fsync
#undef aio_fsync
#define aio_fsync GPOS_BANNED_API(aio_fsync)
#endif

#ifndef ALLOW_access
#undef access
#define access GPOS_BANNED_API(access)
#endif

#ifndef ALLOW_aio_fsync64
#undef aio_fsync64
#define aio_fsync64 GPOS_BANNED_API(aio_fsync64)
#endif

#ifndef ALLOW_acos
#undef acos
#define acos GPOS_BANNED_API(acos)
#endif

#ifndef ALLOW_aio_write
#undef aio_write
#define aio_write GPOS_BANNED_API(aio_write)
#endif

#ifndef ALLOW_acosf
#undef acosf
#define acosf GPOS_BANNED_API(acosf)
#endif

#ifndef ALLOW_aio_write64
#undef aio_write64
#define aio_write64 GPOS_BANNED_API(aio_write64)
#endif

#ifndef ALLOW_acosh
#undef acosh
#define acosh GPOS_BANNED_API(acosh)
#endif

#ifndef ALLOW_alarm
#undef alarm
#define alarm GPOS_BANNED_API(alarm)
#endif

#ifndef ALLOW_acoshf
#undef acoshf
#define acoshf GPOS_BANNED_API(acoshf)
#endif

#ifndef ALLOW_argp_error
#undef argp_error
#define argp_error GPOS_BANNED_API(argp_error)
#endif

#ifndef ALLOW_acoshl
#undef acoshl
#define acoshl GPOS_BANNED_API(acoshl)
#endif

#ifndef ALLOW_argp_failure
#undef argp_failure
#define argp_failure GPOS_BANNED_API(argp_failure)
#endif

#ifndef ALLOW_acosl
#undef acosl
#define acosl GPOS_BANNED_API(acosl)
#endif

#ifndef ALLOW_argz_add_sep
#undef argz_add_sep
#define argz_add_sep GPOS_BANNED_API(argz_add_sep)
#endif

#ifndef ALLOW_addseverity
#undef addseverity
#define addseverity GPOS_BANNED_API(addseverity)
#endif

#ifndef ALLOW_argz_append
#undef argz_append
#define argz_append GPOS_BANNED_API(argz_append)
#endif

#ifndef ALLOW_adjtime
#undef adjtime
#define adjtime GPOS_BANNED_API(adjtime)
#endif

#ifndef ALLOW_argz_create
#undef argz_create
#define argz_create GPOS_BANNED_API(argz_create)
#endif

#ifndef ALLOW_adjtimex
#undef adjtimex
#define adjtimex GPOS_BANNED_API(adjtimex)
#endif

#ifndef ALLOW_argz_replace
#undef argz_replace
#define argz_replace GPOS_BANNED_API(argz_replace)
#endif

#ifndef ALLOW_aio_cancel
#undef aio_cancel
#define aio_cancel GPOS_BANNED_API(aio_cancel)
#endif

#ifndef ALLOW_asctime_r
#undef asctime_r
#define asctime_r GPOS_BANNED_API(asctime_r)
#endif

#ifndef ALLOW_aio_cancel64
#undef aio_cancel64
#define aio_cancel64 GPOS_BANNED_API(aio_cancel64)
#endif

#ifndef ALLOW_asin
#undef asin
#define asin GPOS_BANNED_API(asin)
#endif

#ifndef ALLOW_aio_error
#undef aio_error
#define aio_error GPOS_BANNED_API(aio_error)
#endif

#ifndef ALLOW_asinf
#undef asinf
#define asinf GPOS_BANNED_API(asinf)
#endif

#ifndef ALLOW_aio_error64
#undef aio_error64
#define aio_error64 GPOS_BANNED_API(aio_error64)
#endif

#ifndef ALLOW_asinh
#undef asinh
#define asinh GPOS_BANNED_API(asinh)
#endif

#ifndef ALLOW_aio_init
#undef aio_init
#define aio_init GPOS_BANNED_API(aio_init)
#endif

#ifndef ALLOW_asinhf
#undef asinhf
#define asinhf GPOS_BANNED_API(asinhf)
#endif

#ifndef ALLOW_aio_read
#undef aio_read
#define aio_read GPOS_BANNED_API(aio_read)
#endif

#ifndef ALLOW_asinhl
#undef asinhl
#define asinhl GPOS_BANNED_API(asinhl)
#endif

#ifndef ALLOW_aio_read64
#undef aio_read64
#define aio_read64 GPOS_BANNED_API(aio_read64)
#endif

#ifndef ALLOW_asinl
#undef asinl
#define asinl GPOS_BANNED_API(asinl)
#endif

#ifndef ALLOW_aio_return
#undef aio_return
#define aio_return GPOS_BANNED_API(aio_return)
#endif

#ifndef ALLOW_asprintf
#undef asprintf
#define asprintf GPOS_BANNED_API(asprintf)
#endif

#ifndef ALLOW_aio_return64
#undef aio_return64
#define aio_return64 GPOS_BANNED_API(aio_return64)
#endif

#ifndef ALLOW_assert_perror
#undef assert_perror
#define assert_perror GPOS_BANNED_API(assert_perror)
#endif

#ifndef ALLOW_aio_suspend
#undef aio_suspend
#define aio_suspend GPOS_BANNED_API(aio_suspend)
#endif

#ifndef ALLOW_atexit
#undef atexit
#define atexit GPOS_BANNED_API(atexit)
#endif

#ifndef ALLOW_aio_suspend64
#undef aio_suspend64
#define aio_suspend64 GPOS_BANNED_API(aio_suspend64)
#endif

#ifndef ALLOW_atoi
#undef atoi
#define atoi GPOS_BANNED_API(atoi)
#endif

#ifndef ALLOW_alloca
#undef alloca
#define alloca GPOS_BANNED_API(alloca)
#endif

#ifndef ALLOW_backtrace_symbols_fd
#undef backtrace_symbols_fd
#define backtrace_symbols_fd GPOS_BANNED_API(backtrace_symbols_fd)
#endif

#ifndef ALLOW_alphasort
#undef alphasort
#define alphasort GPOS_BANNED_API(alphasort)
#endif

#ifndef ALLOW_bcopy
#undef bcopy
#define bcopy GPOS_BANNED_API(bcopy)
#endif

#ifndef ALLOW_alphasort64
#undef alphasort64
#define alphasort64 GPOS_BANNED_API(alphasort64)
#endif

#ifndef ALLOW_bind
#undef bind
#define bind GPOS_BANNED_API(bind)
#endif

#ifndef ALLOW_argp_help
#undef argp_help
#define argp_help GPOS_BANNED_API(argp_help)
#endif

#ifndef ALLOW_bind_textdomain_codeset
#undef bind_textdomain_codeset
#define bind_textdomain_codeset GPOS_BANNED_API(bind_textdomain_codeset)
#endif

#ifndef ALLOW_argp_parse
#undef argp_parse
#define argp_parse GPOS_BANNED_API(argp_parse)
#endif

#ifndef ALLOW_brk
#undef brk
#define brk GPOS_BANNED_API(brk)
#endif

#ifndef ALLOW_argp_state_help
#undef argp_state_help
#define argp_state_help GPOS_BANNED_API(argp_state_help)
#endif

#ifndef ALLOW_btowc
#undef btowc
#define btowc GPOS_BANNED_API(btowc)
#endif

#ifndef ALLOW_argp_usage
#undef argp_usage
#define argp_usage GPOS_BANNED_API(argp_usage)
#endif

#ifndef ALLOW_cabs
#undef cabs
#define cabs GPOS_BANNED_API(cabs)
#endif

#ifndef ALLOW_argz_add
#undef argz_add
#define argz_add GPOS_BANNED_API(argz_add)
#endif

#ifndef ALLOW_cabsf
#undef cabsf
#define cabsf GPOS_BANNED_API(cabsf)
#endif

#ifndef ALLOW_argz_count
#undef argz_count
#define argz_count GPOS_BANNED_API(argz_count)
#endif

#ifndef ALLOW_cabsl
#undef cabsl
#define cabsl GPOS_BANNED_API(cabsl)
#endif

#ifndef ALLOW_argz_create_sep
#undef argz_create_sep
#define argz_create_sep GPOS_BANNED_API(argz_create_sep)
#endif

#ifndef ALLOW_cacos
#undef cacos
#define cacos GPOS_BANNED_API(cacos)
#endif

#ifndef ALLOW_argz_delete
#undef argz_delete
#define argz_delete GPOS_BANNED_API(argz_delete)
#endif

#ifndef ALLOW_cacosf
#undef cacosf
#define cacosf GPOS_BANNED_API(cacosf)
#endif

#ifndef ALLOW_argz_extract
#undef argz_extract
#define argz_extract GPOS_BANNED_API(argz_extract)
#endif

#ifndef ALLOW_cacosh
#undef cacosh
#define cacosh GPOS_BANNED_API(cacosh)
#endif

#ifndef ALLOW_argz_insert
#undef argz_insert
#define argz_insert GPOS_BANNED_API(argz_insert)
#endif

#ifndef ALLOW_cacoshf
#undef cacoshf
#define cacoshf GPOS_BANNED_API(cacoshf)
#endif

#ifndef ALLOW_argz_next
#undef argz_next
#define argz_next GPOS_BANNED_API(argz_next)
#endif

#ifndef ALLOW_cacoshl
#undef cacoshl
#define cacoshl GPOS_BANNED_API(cacoshl)
#endif

#ifndef ALLOW_argz_stringify
#undef argz_stringify
#define argz_stringify GPOS_BANNED_API(argz_stringify)
#endif

#ifndef ALLOW_cacosl
#undef cacosl
#define cacosl GPOS_BANNED_API(cacosl)
#endif

#ifndef ALLOW_assert
#undef assert
#define assert GPOS_BANNED_API(assert)
#endif

#ifndef ALLOW_canonicalize_file_name
#undef canonicalize_file_name
#define canonicalize_file_name GPOS_BANNED_API(canonicalize_file_name)
#endif

#ifndef ALLOW_atan
#undef atan
#define atan GPOS_BANNED_API(atan)
#endif

#ifndef ALLOW_carg
#undef carg
#define carg GPOS_BANNED_API(carg)
#endif

#ifndef ALLOW_atan2
#undef atan2
#define atan2 GPOS_BANNED_API(atan2)
#endif

#ifndef ALLOW_cargf
#undef cargf
#define cargf GPOS_BANNED_API(cargf)
#endif

#ifndef ALLOW_atan2f
#undef atan2f
#define atan2f GPOS_BANNED_API(atan2f)
#endif

#ifndef ALLOW_cargl
#undef cargl
#define cargl GPOS_BANNED_API(cargl)
#endif

#ifndef ALLOW_atan2l
#undef atan2l
#define atan2l GPOS_BANNED_API(atan2l)
#endif

#ifndef ALLOW_catan
#undef catan
#define catan GPOS_BANNED_API(catan)
#endif

#ifndef ALLOW_atanf
#undef atanf
#define atanf GPOS_BANNED_API(atanf)
#endif

#ifndef ALLOW_catanf
#undef catanf
#define catanf GPOS_BANNED_API(catanf)
#endif

#ifndef ALLOW_atanh
#undef atanh
#define atanh GPOS_BANNED_API(atanh)
#endif

#ifndef ALLOW_catanh
#undef catanh
#define catanh GPOS_BANNED_API(catanh)
#endif

#ifndef ALLOW_atanhf
#undef atanhf
#define atanhf GPOS_BANNED_API(atanhf)
#endif

#ifndef ALLOW_catanhf
#undef catanhf
#define catanhf GPOS_BANNED_API(catanhf)
#endif

#ifndef ALLOW_atanhl
#undef atanhl
#define atanhl GPOS_BANNED_API(atanhl)
#endif

#ifndef ALLOW_catanhl
#undef catanhl
#define catanhl GPOS_BANNED_API(catanhl)
#endif

#ifndef ALLOW_atanl
#undef atanl
#define atanl GPOS_BANNED_API(atanl)
#endif

#ifndef ALLOW_catanl
#undef catanl
#define catanl GPOS_BANNED_API(catanl)
#endif

#ifndef ALLOW_atof
#undef atof
#define atof GPOS_BANNED_API(atof)
#endif

#ifndef ALLOW_catgets
#undef catgets
#define catgets GPOS_BANNED_API(catgets)
#endif

#ifndef ALLOW_atol
#undef atol
#define atol GPOS_BANNED_API(atol)
#endif

#ifndef ALLOW_cbc_crypt
#undef cbc_crypt
#define cbc_crypt GPOS_BANNED_API(cbc_crypt)
#endif

#ifndef ALLOW_atoll
#undef atoll
#define atoll GPOS_BANNED_API(atoll)
#endif

#ifndef ALLOW_cbrt
#undef cbrt
#define cbrt GPOS_BANNED_API(cbrt)
#endif

#ifndef ALLOW_backtrace
#undef backtrace
#define backtrace GPOS_BANNED_API(backtrace)
#endif

#ifndef ALLOW_cbrtf
#undef cbrtf
#define cbrtf GPOS_BANNED_API(cbrtf)
#endif

#ifndef ALLOW_backtrace_symbols
#undef backtrace_symbols
#define backtrace_symbols GPOS_BANNED_API(backtrace_symbols)
#endif

#ifndef ALLOW_cbrtl
#undef cbrtl
#define cbrtl GPOS_BANNED_API(cbrtl)
#endif

#ifndef ALLOW_basename
#undef basename
#define basename GPOS_BANNED_API(basename)
#endif

#ifndef ALLOW_ceil
#undef ceil
#define ceil GPOS_BANNED_API(ceil)
#endif

#ifndef ALLOW_bcmp
#undef bcmp
#define bcmp GPOS_BANNED_API(bcmp)
#endif

#ifndef ALLOW_ceilf
#undef ceilf
#define ceilf GPOS_BANNED_API(ceilf)
#endif

#ifndef ALLOW_bindtextdomain
#undef bindtextdomain
#define bindtextdomain GPOS_BANNED_API(bindtextdomain)
#endif

#ifndef ALLOW_ceill
#undef ceill
#define ceill GPOS_BANNED_API(ceill)
#endif

#ifndef ALLOW_bsearch
#undef bsearch
#define bsearch GPOS_BANNED_API(bsearch)
#endif

#ifndef ALLOW_cfgetispeed
#undef cfgetispeed
#define cfgetispeed GPOS_BANNED_API(cfgetispeed)
#endif

#ifndef ALLOW_bzero
#undef bzero
#define bzero GPOS_BANNED_API(bzero)
#endif

#ifndef ALLOW_cfgetospeed
#undef cfgetospeed
#define cfgetospeed GPOS_BANNED_API(cfgetospeed)
#endif

#ifndef ALLOW_calloc
#undef calloc
#define calloc GPOS_BANNED_API(calloc)
#endif

#ifndef ALLOW_cfmakeraw
#undef cfmakeraw
#define cfmakeraw GPOS_BANNED_API(cfmakeraw)
#endif

#ifndef ALLOW_casin
#undef casin
#define casin GPOS_BANNED_API(casin)
#endif

#ifndef ALLOW_cfree
#undef cfree
#define cfree GPOS_BANNED_API(cfree)
#endif

#ifndef ALLOW_casinf
#undef casinf
#define casinf GPOS_BANNED_API(casinf)
#endif

#ifndef ALLOW_cfsetispeed
#undef cfsetispeed
#define cfsetispeed GPOS_BANNED_API(cfsetispeed)
#endif

#ifndef ALLOW_casinh
#undef casinh
#define casinh GPOS_BANNED_API(casinh)
#endif

#ifndef ALLOW_cfsetospeed
#undef cfsetospeed
#define cfsetospeed GPOS_BANNED_API(cfsetospeed)
#endif

#ifndef ALLOW_casinhf
#undef casinhf
#define casinhf GPOS_BANNED_API(casinhf)
#endif

#ifndef ALLOW_chmod
#undef chmod
#define chmod GPOS_BANNED_API(chmod)
#endif

#ifndef ALLOW_casinhl
#undef casinhl
#define casinhl GPOS_BANNED_API(casinhl)
#endif

#ifndef ALLOW_chown
#undef chown
#define chown GPOS_BANNED_API(chown)
#endif

#ifndef ALLOW_casinl
#undef casinl
#define casinl GPOS_BANNED_API(casinl)
#endif

#ifndef ALLOW_cimag
#undef cimag
#define cimag GPOS_BANNED_API(cimag)
#endif

#ifndef ALLOW_catclose
#undef catclose
#define catclose GPOS_BANNED_API(catclose)
#endif

#ifndef ALLOW_cimagf
#undef cimagf
#define cimagf GPOS_BANNED_API(cimagf)
#endif

#ifndef ALLOW_catopen
#undef catopen
#define catopen GPOS_BANNED_API(catopen)
#endif

#ifndef ALLOW_cimagl
#undef cimagl
#define cimagl GPOS_BANNED_API(cimagl)
#endif

#ifndef ALLOW_ccos
#undef ccos
#define ccos GPOS_BANNED_API(ccos)
#endif

#ifndef ALLOW_clog
#undef clog
#define clog GPOS_BANNED_API(clog)
#endif

#ifndef ALLOW_ccosf
#undef ccosf
#define ccosf GPOS_BANNED_API(ccosf)
#endif

#ifndef ALLOW_clogf
#undef clogf
#define clogf GPOS_BANNED_API(clogf)
#endif

#ifndef ALLOW_ccosh
#undef ccosh
#define ccosh GPOS_BANNED_API(ccosh)
#endif

#ifndef ALLOW_clogl
#undef clogl
#define clogl GPOS_BANNED_API(clogl)
#endif

#ifndef ALLOW_ccoshf
#undef ccoshf
#define ccoshf GPOS_BANNED_API(ccoshf)
#endif

#ifndef ALLOW_closedir
#undef closedir
#define closedir GPOS_BANNED_API(closedir)
#endif

#ifndef ALLOW_ccoshl
#undef ccoshl
#define ccoshl GPOS_BANNED_API(ccoshl)
#endif

#ifndef ALLOW_confstr
#undef confstr
#define confstr GPOS_BANNED_API(confstr)
#endif

#ifndef ALLOW_ccosl
#undef ccosl
#define ccosl GPOS_BANNED_API(ccosl)
#endif

#ifndef ALLOW_cos
#undef cos
#define cos GPOS_BANNED_API(cos)
#endif

#ifndef ALLOW_cexp
#undef cexp
#define cexp GPOS_BANNED_API(cexp)
#endif

#ifndef ALLOW_cosf
#undef cosf
#define cosf GPOS_BANNED_API(cosf)
#endif

#ifndef ALLOW_cexpf
#undef cexpf
#define cexpf GPOS_BANNED_API(cexpf)
#endif

#ifndef ALLOW_cosh
#undef cosh
#define cosh GPOS_BANNED_API(cosh)
#endif

#ifndef ALLOW_cexpl
#undef cexpl
#define cexpl GPOS_BANNED_API(cexpl)
#endif

#ifndef ALLOW_coshf
#undef coshf
#define coshf GPOS_BANNED_API(coshf)
#endif

#ifndef ALLOW_cfsetspeed
#undef cfsetspeed
#define cfsetspeed GPOS_BANNED_API(cfsetspeed)
#endif

#ifndef ALLOW_coshl
#undef coshl
#define coshl GPOS_BANNED_API(coshl)
#endif

#ifndef ALLOW_chdir
#undef chdir
#define chdir GPOS_BANNED_API(chdir)
#endif

#ifndef ALLOW_cosl
#undef cosl
#define cosl GPOS_BANNED_API(cosl)
#endif

#ifndef ALLOW_clearenv
#undef clearenv
#define clearenv GPOS_BANNED_API(clearenv)
#endif

#ifndef ALLOW_cpow
#undef cpow
#define cpow GPOS_BANNED_API(cpow)
#endif

#ifndef ALLOW_clearerr
#undef clearerr
#define clearerr GPOS_BANNED_API(clearerr)
#endif

#ifndef ALLOW_cpowf
#undef cpowf
#define cpowf GPOS_BANNED_API(cpowf)
#endif

#ifndef ALLOW_clearerr_unlocked
#undef clearerr_unlocked
#define clearerr_unlocked GPOS_BANNED_API(clearerr_unlocked)
#endif

#ifndef ALLOW_cpowl
#undef cpowl
#define cpowl GPOS_BANNED_API(cpowl)
#endif

#ifndef ALLOW_clock
#undef clock
#define clock GPOS_BANNED_API(clock)
#endif

#ifndef ALLOW_CPU_ISSET
#undef CPU_ISSET
#define CPU_ISSET GPOS_BANNED_API(CPU_ISSET)
#endif

#ifndef ALLOW_clog10
#undef clog10
#define clog10 GPOS_BANNED_API(clog10)
#endif

#ifndef ALLOW_CPU_SET
#undef CPU_SET
#define CPU_SET GPOS_BANNED_API(CPU_SET)
#endif

#ifndef ALLOW_clog10f
#undef clog10f
#define clog10f GPOS_BANNED_API(clog10f)
#endif

#ifndef ALLOW_CPU_ZERO
#undef CPU_ZERO
#define CPU_ZERO GPOS_BANNED_API(CPU_ZERO)
#endif

#ifndef ALLOW_clog10l
#undef clog10l
#define clog10l GPOS_BANNED_API(clog10l)
#endif

#ifndef ALLOW_creal
#undef creal
#define creal GPOS_BANNED_API(creal)
#endif

#ifndef ALLOW_close
#undef close
#define close GPOS_BANNED_API(close)
#endif

#ifndef ALLOW_crealf
#undef crealf
#define crealf GPOS_BANNED_API(crealf)
#endif

#ifndef ALLOW_closelog
#undef closelog
#define closelog GPOS_BANNED_API(closelog)
#endif

#ifndef ALLOW_creall
#undef creall
#define creall GPOS_BANNED_API(creall)
#endif

#ifndef ALLOW_conj
#undef conj
#define conj GPOS_BANNED_API(conj)
#endif

#ifndef ALLOW_creat
#undef creat
#define creat GPOS_BANNED_API(creat)
#endif

#ifndef ALLOW_conjf
#undef conjf
#define conjf GPOS_BANNED_API(conjf)
#endif

#ifndef ALLOW_creat64
#undef creat64
#define creat64 GPOS_BANNED_API(creat64)
#endif

#ifndef ALLOW_conjl
#undef conjl
#define conjl GPOS_BANNED_API(conjl)
#endif

#ifndef ALLOW_crypt_r
#undef crypt_r
#define crypt_r GPOS_BANNED_API(crypt_r)
#endif

#ifndef ALLOW_connect
#undef connect
#define connect GPOS_BANNED_API(connect)
#endif

#ifndef ALLOW_csin
#undef csin
#define csin GPOS_BANNED_API(csin)
#endif

#ifndef ALLOW_copysign
#undef copysign
#define copysign GPOS_BANNED_API(copysign)
#endif

#ifndef ALLOW_csinf
#undef csinf
#define csinf GPOS_BANNED_API(csinf)
#endif

#ifndef ALLOW_copysignf
#undef copysignf
#define copysignf GPOS_BANNED_API(copysignf)
#endif

#ifndef ALLOW_csinh
#undef csinh
#define csinh GPOS_BANNED_API(csinh)
#endif

#ifndef ALLOW_copysignl
#undef copysignl
#define copysignl GPOS_BANNED_API(copysignl)
#endif

#ifndef ALLOW_csinhf
#undef csinhf
#define csinhf GPOS_BANNED_API(csinhf)
#endif

#ifndef ALLOW_cproj
#undef cproj
#define cproj GPOS_BANNED_API(cproj)
#endif

#ifndef ALLOW_csinhl
#undef csinhl
#define csinhl GPOS_BANNED_API(csinhl)
#endif

#ifndef ALLOW_cprojf
#undef cprojf
#define cprojf GPOS_BANNED_API(cprojf)
#endif

#ifndef ALLOW_csinl
#undef csinl
#define csinl GPOS_BANNED_API(csinl)
#endif

#ifndef ALLOW_cprojl
#undef cprojl
#define cprojl GPOS_BANNED_API(cprojl)
#endif

#ifndef ALLOW_csqrt
#undef csqrt
#define csqrt GPOS_BANNED_API(csqrt)
#endif

#ifndef ALLOW_CPU_CLR
#undef CPU_CLR
#define CPU_CLR GPOS_BANNED_API(CPU_CLR)
#endif

#ifndef ALLOW_csqrtf
#undef csqrtf
#define csqrtf GPOS_BANNED_API(csqrtf)
#endif

#ifndef ALLOW_ctan
#undef ctan
#define ctan GPOS_BANNED_API(ctan)
#endif

#ifndef ALLOW_csqrtl
#undef csqrtl
#define csqrtl GPOS_BANNED_API(csqrtl)
#endif

#ifndef ALLOW_ctanf
#undef ctanf
#define ctanf GPOS_BANNED_API(ctanf)
#endif

#ifndef ALLOW_ctime_r
#undef ctime_r
#define ctime_r GPOS_BANNED_API(ctime_r)
#endif

#ifndef ALLOW_ctanh
#undef ctanh
#define ctanh GPOS_BANNED_API(ctanh)
#endif

#ifndef ALLOW_cuserid
#undef cuserid
#define cuserid GPOS_BANNED_API(cuserid)
#endif

#ifndef ALLOW_ctanhf
#undef ctanhf
#define ctanhf GPOS_BANNED_API(ctanhf)
#endif

#ifndef ALLOW_dgettext
#undef dgettext
#define dgettext GPOS_BANNED_API(dgettext)
#endif

#ifndef ALLOW_ctanhl
#undef ctanhl
#define ctanhl GPOS_BANNED_API(ctanhl)
#endif

#ifndef ALLOW_dirfd
#undef dirfd
#define dirfd GPOS_BANNED_API(dirfd)
#endif

#ifndef ALLOW_ctanl
#undef ctanl
#define ctanl GPOS_BANNED_API(ctanl)
#endif

#ifndef ALLOW_div
#undef div
#define div GPOS_BANNED_API(div)
#endif

#ifndef ALLOW_ctermid
#undef ctermid
#define ctermid GPOS_BANNED_API(ctermid)
#endif

#ifndef ALLOW_dngettext
#undef dngettext
#define dngettext GPOS_BANNED_API(dngettext)
#endif

#ifndef ALLOW_dcgettext
#undef dcgettext
#define dcgettext GPOS_BANNED_API(dcgettext)
#endif

#ifndef ALLOW_dup
#undef dup
#define dup GPOS_BANNED_API(dup)
#endif

#ifndef ALLOW_dcngettext
#undef dcngettext
#define dcngettext GPOS_BANNED_API(dcngettext)
#endif

#ifndef ALLOW_dup2
#undef dup2
#define dup2 GPOS_BANNED_API(dup2)
#endif

#ifndef ALLOW_DES_FAILED
#undef DES_FAILED
#define DES_FAILED GPOS_BANNED_API(DES_FAILED)
#endif

#ifndef ALLOW_ecb_crypt
#undef ecb_crypt
#define ecb_crypt GPOS_BANNED_API(ecb_crypt)
#endif

#ifndef ALLOW_des_setparity
#undef des_setparity
#define des_setparity GPOS_BANNED_API(des_setparity)
#endif

#ifndef ALLOW_ecvt_r
#undef ecvt_r
#define ecvt_r GPOS_BANNED_API(ecvt_r)
#endif

#ifndef ALLOW_difftime
#undef difftime
#define difftime GPOS_BANNED_API(difftime)
#endif

#ifndef ALLOW_endfsent
#undef endfsent
#define endfsent GPOS_BANNED_API(endfsent)
#endif

#ifndef ALLOW_dirname
#undef dirname
#define dirname GPOS_BANNED_API(dirname)
#endif

#ifndef ALLOW_endgrent
#undef endgrent
#define endgrent GPOS_BANNED_API(endgrent)
#endif

#ifndef ALLOW_drand48_r
#undef drand48_r
#define drand48_r GPOS_BANNED_API(drand48_r)
#endif

#ifndef ALLOW_endmntent
#undef endmntent
#define endmntent GPOS_BANNED_API(endmntent)
#endif

#ifndef ALLOW_drem
#undef drem
#define drem GPOS_BANNED_API(drem)
#endif

#ifndef ALLOW_endnetent
#undef endnetent
#define endnetent GPOS_BANNED_API(endnetent)
#endif

#ifndef ALLOW_dremf
#undef dremf
#define dremf GPOS_BANNED_API(dremf)
#endif

#ifndef ALLOW_endpwent
#undef endpwent
#define endpwent GPOS_BANNED_API(endpwent)
#endif

#ifndef ALLOW_dreml
#undef dreml
#define dreml GPOS_BANNED_API(dreml)
#endif

#ifndef ALLOW_endutent
#undef endutent
#define endutent GPOS_BANNED_API(endutent)
#endif

#ifndef ALLOW_DTTOIF
#undef DTTOIF
#define DTTOIF GPOS_BANNED_API(DTTOIF)
#endif

#ifndef ALLOW_endutxent
#undef endutxent
#define endutxent GPOS_BANNED_API(endutxent)
#endif

#ifndef ALLOW_encrypt_r
#undef encrypt_r
#define encrypt_r GPOS_BANNED_API(encrypt_r)
#endif

#ifndef ALLOW_envz_add
#undef envz_add
#define envz_add GPOS_BANNED_API(envz_add)
#endif

#ifndef ALLOW_endhostent
#undef endhostent
#define endhostent GPOS_BANNED_API(endhostent)
#endif

#ifndef ALLOW_erand48_r
#undef erand48_r
#define erand48_r GPOS_BANNED_API(erand48_r)
#endif

#ifndef ALLOW_endnetgrent
#undef endnetgrent
#define endnetgrent GPOS_BANNED_API(endnetgrent)
#endif

#ifndef ALLOW_erf
#undef erf
#define erf GPOS_BANNED_API(erf)
#endif

#ifndef ALLOW_endprotoent
#undef endprotoent
#define endprotoent GPOS_BANNED_API(endprotoent)
#endif

#ifndef ALLOW_erff
#undef erff
#define erff GPOS_BANNED_API(erff)
#endif

#ifndef ALLOW_endservent
#undef endservent
#define endservent GPOS_BANNED_API(endservent)
#endif

#ifndef ALLOW_erfl
#undef erfl
#define erfl GPOS_BANNED_API(erfl)
#endif

#ifndef ALLOW_envz_entry
#undef envz_entry
#define envz_entry GPOS_BANNED_API(envz_entry)
#endif

#ifndef ALLOW_err
#undef err
#define err GPOS_BANNED_API(err)
#endif

#ifndef ALLOW_envz_get
#undef envz_get
#define envz_get GPOS_BANNED_API(envz_get)
#endif

#ifndef ALLOW_error_at_line
#undef error_at_line
#define error_at_line GPOS_BANNED_API(error_at_line)
#endif

#ifndef ALLOW_envz_merge
#undef envz_merge
#define envz_merge GPOS_BANNED_API(envz_merge)
#endif

#ifndef ALLOW_errx
#undef errx
#define errx GPOS_BANNED_API(errx)
#endif

#ifndef ALLOW_envz_strip
#undef envz_strip
#define envz_strip GPOS_BANNED_API(envz_strip)
#endif

#ifndef ALLOW_execl
#undef execl
#define execl GPOS_BANNED_API(execl)
#endif

#ifndef ALLOW_erfc
#undef erfc
#define erfc GPOS_BANNED_API(erfc)
#endif

#ifndef ALLOW_execlp
#undef execlp
#define execlp GPOS_BANNED_API(execlp)
#endif

#ifndef ALLOW_erfcf
#undef erfcf
#define erfcf GPOS_BANNED_API(erfcf)
#endif

#ifndef ALLOW_execv
#undef execv
#define execv GPOS_BANNED_API(execv)
#endif

#ifndef ALLOW_erfcl
#undef erfcl
#define erfcl GPOS_BANNED_API(erfcl)
#endif

#ifndef ALLOW_execvp
#undef execvp
#define execvp GPOS_BANNED_API(execvp)
#endif

#ifndef ALLOW_error
#undef error
#define error GPOS_BANNED_API(error)
#endif

#ifndef ALLOW_exp
#undef exp
#define exp GPOS_BANNED_API(exp)
#endif

#ifndef ALLOW_execle
#undef execle
#define execle GPOS_BANNED_API(execle)
#endif

#ifndef ALLOW_exp2
#undef exp2
#define exp2 GPOS_BANNED_API(exp2)
#endif

#ifndef ALLOW_execve
#undef execve
#define execve GPOS_BANNED_API(execve)
#endif

#ifndef ALLOW_exp2f
#undef exp2f
#define exp2f GPOS_BANNED_API(exp2f)
#endif

#ifndef ALLOW_exit
#undef exit
#define exit GPOS_BANNED_API(exit)
#endif

#ifndef ALLOW_exp2l
#undef exp2l
#define exp2l GPOS_BANNED_API(exp2l)
#endif

#ifndef ALLOW_exp10
#undef exp10
#define exp10 GPOS_BANNED_API(exp10)
#endif

#ifndef ALLOW_expf
#undef expf
#define expf GPOS_BANNED_API(expf)
#endif

#ifndef ALLOW_exp10f
#undef exp10f
#define exp10f GPOS_BANNED_API(exp10f)
#endif

#ifndef ALLOW_expl
#undef expl
#define expl GPOS_BANNED_API(expl)
#endif

#ifndef ALLOW_exp10l
#undef exp10l
#define exp10l GPOS_BANNED_API(exp10l)
#endif

#ifndef ALLOW_expm1
#undef expm1
#define expm1 GPOS_BANNED_API(expm1)
#endif

#ifndef ALLOW_fabs
#undef fabs
#define fabs GPOS_BANNED_API(fabs)
#endif

#ifndef ALLOW_expm1f
#undef expm1f
#define expm1f GPOS_BANNED_API(expm1f)
#endif

#ifndef ALLOW_fabsf
#undef fabsf
#define fabsf GPOS_BANNED_API(fabsf)
#endif

#ifndef ALLOW_expm1l
#undef expm1l
#define expm1l GPOS_BANNED_API(expm1l)
#endif

#ifndef ALLOW_fabsl
#undef fabsl
#define fabsl GPOS_BANNED_API(fabsl)
#endif

#ifndef ALLOW_fchmod
#undef fchmod
#define fchmod GPOS_BANNED_API(fchmod)
#endif

#ifndef ALLOW_fchdir
#undef fchdir
#define fchdir GPOS_BANNED_API(fchdir)
#endif

#ifndef ALLOW_fchown
#undef fchown
#define fchown GPOS_BANNED_API(fchown)
#endif

#ifndef ALLOW_fclose
#undef fclose
#define fclose GPOS_BANNED_API(fclose)
#endif

#ifndef ALLOW_fclean
#undef fclean
#define fclean GPOS_BANNED_API(fclean)
#endif

#ifndef ALLOW_fcvt_r
#undef fcvt_r
#define fcvt_r GPOS_BANNED_API(fcvt_r)
#endif

#ifndef ALLOW_fcloseall
#undef fcloseall
#define fcloseall GPOS_BANNED_API(fcloseall)
#endif

#ifndef ALLOW_FD_CLR
#undef FD_CLR
#define FD_CLR GPOS_BANNED_API(FD_CLR)
#endif

#ifndef ALLOW_fcntl
#undef fcntl
#define fcntl GPOS_BANNED_API(fcntl)
#endif

#ifndef ALLOW_fdim
#undef fdim
#define fdim GPOS_BANNED_API(fdim)
#endif

#ifndef ALLOW_FD_ISSET
#undef FD_ISSET
#define FD_ISSET GPOS_BANNED_API(FD_ISSET)
#endif

#ifndef ALLOW_fdimf
#undef fdimf
#define fdimf GPOS_BANNED_API(fdimf)
#endif

#ifndef ALLOW_FD_SET
#undef FD_SET
#define FD_SET GPOS_BANNED_API(FD_SET)
#endif

#ifndef ALLOW_fdiml
#undef fdiml
#define fdiml GPOS_BANNED_API(fdiml)
#endif

#ifndef ALLOW_FD_ZERO
#undef FD_ZERO
#define FD_ZERO GPOS_BANNED_API(FD_ZERO)
#endif

#ifndef ALLOW_fdopen
#undef fdopen
#define fdopen GPOS_BANNED_API(fdopen)
#endif

#ifndef ALLOW_fdatasync
#undef fdatasync
#define fdatasync GPOS_BANNED_API(fdatasync)
#endif

#ifndef ALLOW_fedisableexcept
#undef fedisableexcept
#define fedisableexcept GPOS_BANNED_API(fedisableexcept)
#endif

#ifndef ALLOW_fdopendir
#undef fdopendir
#define fdopendir GPOS_BANNED_API(fdopendir)
#endif

#ifndef ALLOW_fegetenv
#undef fegetenv
#define fegetenv GPOS_BANNED_API(fegetenv)
#endif

#ifndef ALLOW_feclearexcept
#undef feclearexcept
#define feclearexcept GPOS_BANNED_API(feclearexcept)
#endif

#ifndef ALLOW_fegetexcept
#undef fegetexcept
#define fegetexcept GPOS_BANNED_API(fegetexcept)
#endif

#ifndef ALLOW_feenableexcept
#undef feenableexcept
#define feenableexcept GPOS_BANNED_API(feenableexcept)
#endif

#ifndef ALLOW_fegetexceptflag
#undef fegetexceptflag
#define fegetexceptflag GPOS_BANNED_API(fegetexceptflag)
#endif

#ifndef ALLOW_fegetround
#undef fegetround
#define fegetround GPOS_BANNED_API(fegetround)
#endif

#ifndef ALLOW_feof
#undef feof
#define feof GPOS_BANNED_API(feof)
#endif

#ifndef ALLOW_feholdexcept
#undef feholdexcept
#define feholdexcept GPOS_BANNED_API(feholdexcept)
#endif

#ifndef ALLOW_feof_unlocked
#undef feof_unlocked
#define feof_unlocked GPOS_BANNED_API(feof_unlocked)
#endif

#ifndef ALLOW_fesetround
#undef fesetround
#define fesetround GPOS_BANNED_API(fesetround)
#endif

#ifndef ALLOW_feraiseexcept
#undef feraiseexcept
#define feraiseexcept GPOS_BANNED_API(feraiseexcept)
#endif

#ifndef ALLOW_feupdateenv
#undef feupdateenv
#define feupdateenv GPOS_BANNED_API(feupdateenv)
#endif

#ifndef ALLOW_ferror
#undef ferror
#define ferror GPOS_BANNED_API(ferror)
#endif

#ifndef ALLOW_fgetc
#undef fgetc
#define fgetc GPOS_BANNED_API(fgetc)
#endif

#ifndef ALLOW_ferror_unlocked
#undef ferror_unlocked
#define ferror_unlocked GPOS_BANNED_API(ferror_unlocked)
#endif

#ifndef ALLOW_fgetc_unlocked
#undef fgetc_unlocked
#define fgetc_unlocked GPOS_BANNED_API(fgetc_unlocked)
#endif

#ifndef ALLOW_fesetenv
#undef fesetenv
#define fesetenv GPOS_BANNED_API(fesetenv)
#endif

#ifndef ALLOW_fgetgrent_r
#undef fgetgrent_r
#define fgetgrent_r GPOS_BANNED_API(fgetgrent_r)
#endif

#ifndef ALLOW_fesetexceptflag
#undef fesetexceptflag
#define fesetexceptflag GPOS_BANNED_API(fesetexceptflag)
#endif

#ifndef ALLOW_fgetpwent_r
#undef fgetpwent_r
#define fgetpwent_r GPOS_BANNED_API(fgetpwent_r)
#endif

#ifndef ALLOW_fetestexcept
#undef fetestexcept
#define fetestexcept GPOS_BANNED_API(fetestexcept)
#endif

#ifndef ALLOW_fgets
#undef fgets
#define fgets GPOS_BANNED_API(fgets)
#endif

#ifndef ALLOW_fflush
#undef fflush
#define fflush GPOS_BANNED_API(fflush)
#endif

#ifndef ALLOW_fgets_unlocked
#undef fgets_unlocked
#define fgets_unlocked GPOS_BANNED_API(fgets_unlocked)
#endif

#ifndef ALLOW_fflush_unlocked
#undef fflush_unlocked
#define fflush_unlocked GPOS_BANNED_API(fflush_unlocked)
#endif

#ifndef ALLOW_fileno
#undef fileno
#define fileno GPOS_BANNED_API(fileno)
#endif

#ifndef ALLOW_fgetpos
#undef fgetpos
#define fgetpos GPOS_BANNED_API(fgetpos)
#endif

#ifndef ALLOW_fileno_unlocked
#undef fileno_unlocked
#define fileno_unlocked GPOS_BANNED_API(fileno_unlocked)
#endif

#ifndef ALLOW_fgetpos64
#undef fgetpos64
#define fgetpos64 GPOS_BANNED_API(fgetpos64)
#endif

#ifndef ALLOW_finite
#undef finite
#define finite GPOS_BANNED_API(finite)
#endif

#ifndef ALLOW_fgetwc
#undef fgetwc
#define fgetwc GPOS_BANNED_API(fgetwc)
#endif

#ifndef ALLOW_finitef
#undef finitef
#define finitef GPOS_BANNED_API(finitef)
#endif

#ifndef ALLOW_fgetwc_unlocked
#undef fgetwc_unlocked
#define fgetwc_unlocked GPOS_BANNED_API(fgetwc_unlocked)
#endif

#ifndef ALLOW_finitel
#undef finitel
#define finitel GPOS_BANNED_API(finitel)
#endif

#ifndef ALLOW_fgetws
#undef fgetws
#define fgetws GPOS_BANNED_API(fgetws)
#endif

#ifndef ALLOW_flockfile
#undef flockfile
#define flockfile GPOS_BANNED_API(flockfile)
#endif

#ifndef ALLOW_fgetws_unlocked
#undef fgetws_unlocked
#define fgetws_unlocked GPOS_BANNED_API(fgetws_unlocked)
#endif

#ifndef ALLOW_fmemopen
#undef fmemopen
#define fmemopen GPOS_BANNED_API(fmemopen)
#endif

#ifndef ALLOW_floor
#undef floor
#define floor GPOS_BANNED_API(floor)
#endif

#ifndef ALLOW_fnmatch
#undef fnmatch
#define fnmatch GPOS_BANNED_API(fnmatch)
#endif

#ifndef ALLOW_floorf
#undef floorf
#define floorf GPOS_BANNED_API(floorf)
#endif

#ifndef ALLOW_forkpty
#undef forkpty
#define forkpty GPOS_BANNED_API(forkpty)
#endif

#ifndef ALLOW_floorl
#undef floorl
#define floorl GPOS_BANNED_API(floorl)
#endif

#ifndef ALLOW_fpathconf
#undef fpathconf
#define fpathconf GPOS_BANNED_API(fpathconf)
#endif

#ifndef ALLOW_fma
#undef fma
#define fma GPOS_BANNED_API(fma)
#endif

#ifndef ALLOW_fprintf
#undef fprintf
#define fprintf GPOS_BANNED_API(fprintf)
#endif

#ifndef ALLOW_fmaf
#undef fmaf
#define fmaf GPOS_BANNED_API(fmaf)
#endif

#ifndef ALLOW_fputwc
#undef fputwc
#define fputwc GPOS_BANNED_API(fputwc)
#endif

#ifndef ALLOW_fmal
#undef fmal
#define fmal GPOS_BANNED_API(fmal)
#endif

#ifndef ALLOW_fputwc_unlocked
#undef fputwc_unlocked
#define fputwc_unlocked GPOS_BANNED_API(fputwc_unlocked)
#endif

#ifndef ALLOW_fmax
#undef fmax
#define fmax GPOS_BANNED_API(fmax)
#endif

#ifndef ALLOW_fputws
#undef fputws
#define fputws GPOS_BANNED_API(fputws)
#endif

#ifndef ALLOW_fmaxf
#undef fmaxf
#define fmaxf GPOS_BANNED_API(fmaxf)
#endif

#ifndef ALLOW_fputws_unlocked
#undef fputws_unlocked
#define fputws_unlocked GPOS_BANNED_API(fputws_unlocked)
#endif

#ifndef ALLOW_fmaxl
#undef fmaxl
#define fmaxl GPOS_BANNED_API(fmaxl)
#endif

#ifndef ALLOW_freopen
#undef freopen
#define freopen GPOS_BANNED_API(freopen)
#endif

#ifndef ALLOW_fmin
#undef fmin
#define fmin GPOS_BANNED_API(fmin)
#endif

#ifndef ALLOW_freopen64
#undef freopen64
#define freopen64 GPOS_BANNED_API(freopen64)
#endif

#ifndef ALLOW_fminf
#undef fminf
#define fminf GPOS_BANNED_API(fminf)
#endif

#ifndef ALLOW_frexp
#undef frexp
#define frexp GPOS_BANNED_API(frexp)
#endif

#ifndef ALLOW_fminl
#undef fminl
#define fminl GPOS_BANNED_API(fminl)
#endif

#ifndef ALLOW_frexpf
#undef frexpf
#define frexpf GPOS_BANNED_API(frexpf)
#endif

#ifndef ALLOW_fmod
#undef fmod
#define fmod GPOS_BANNED_API(fmod)
#endif

#ifndef ALLOW_frexpl
#undef frexpl
#define frexpl GPOS_BANNED_API(frexpl)
#endif

#ifndef ALLOW_fmodf
#undef fmodf
#define fmodf GPOS_BANNED_API(fmodf)
#endif

#ifndef ALLOW_fscanf
#undef fscanf
#define fscanf GPOS_BANNED_API(fscanf)
#endif

#ifndef ALLOW_fmodl
#undef fmodl
#define fmodl GPOS_BANNED_API(fmodl)
#endif

#ifndef ALLOW_fseeko
#undef fseeko
#define fseeko GPOS_BANNED_API(fseeko)
#endif

#ifndef ALLOW_fmtmsg
#undef fmtmsg
#define fmtmsg GPOS_BANNED_API(fmtmsg)
#endif

#ifndef ALLOW_fseeko64
#undef fseeko64
#define fseeko64 GPOS_BANNED_API(fseeko64)
#endif

#ifndef ALLOW_fopen
#undef fopen
#define fopen GPOS_BANNED_API(fopen)
#endif

#ifndef ALLOW_fsync
#undef fsync
#define fsync GPOS_BANNED_API(fsync)
#endif

#ifndef ALLOW_fopen64
#undef fopen64
#define fopen64 GPOS_BANNED_API(fopen64)
#endif

#ifndef ALLOW_ftell
#undef ftell
#define ftell GPOS_BANNED_API(ftell)
#endif

#ifndef ALLOW_fopencookie
#undef fopencookie
#define fopencookie GPOS_BANNED_API(fopencookie)
#endif

#ifndef ALLOW_ftw
#undef ftw
#define ftw GPOS_BANNED_API(ftw)
#endif

#ifndef ALLOW_fork
#undef fork
#define fork GPOS_BANNED_API(fork)
#endif

#ifndef ALLOW_ftw64
#undef ftw64
#define ftw64 GPOS_BANNED_API(ftw64)
#endif

#ifndef ALLOW_fpclassify
#undef fpclassify
#define fpclassify GPOS_BANNED_API(fpclassify)
#endif

#ifndef ALLOW_futimes
#undef futimes
#define futimes GPOS_BANNED_API(futimes)
#endif

#ifndef ALLOW_fputc
#undef fputc
#define fputc GPOS_BANNED_API(fputc)
#endif

#ifndef ALLOW_fwide
#undef fwide
#define fwide GPOS_BANNED_API(fwide)
#endif

#ifndef ALLOW_fputc_unlocked
#undef fputc_unlocked
#define fputc_unlocked GPOS_BANNED_API(fputc_unlocked)
#endif

#ifndef ALLOW_fwrite
#undef fwrite
#define fwrite GPOS_BANNED_API(fwrite)
#endif

#ifndef ALLOW_fputs
#undef fputs
#define fputs GPOS_BANNED_API(fputs)
#endif

#ifndef ALLOW_fwrite_unlocked
#undef fwrite_unlocked
#define fwrite_unlocked GPOS_BANNED_API(fwrite_unlocked)
#endif

#ifndef ALLOW_fputs_unlocked
#undef fputs_unlocked
#define fputs_unlocked GPOS_BANNED_API(fputs_unlocked)
#endif

#ifndef ALLOW_gamma
#undef gamma
#define gamma GPOS_BANNED_API(gamma)
#endif

#ifndef ALLOW_fread
#undef fread
#define fread GPOS_BANNED_API(fread)
#endif

#ifndef ALLOW_gammaf
#undef gammaf
#define gammaf GPOS_BANNED_API(gammaf)
#endif

#ifndef ALLOW_fread_unlocked
#undef fread_unlocked
#define fread_unlocked GPOS_BANNED_API(fread_unlocked)
#endif

#ifndef ALLOW_gammal
#undef gammal
#define gammal GPOS_BANNED_API(gammal)
#endif

#ifndef ALLOW_free
#undef free
#define free GPOS_BANNED_API(free)
#endif

#ifndef ALLOW_get_avphys_pages
#undef get_avphys_pages
#define get_avphys_pages GPOS_BANNED_API(get_avphys_pages)
#endif

#ifndef ALLOW_fseek
#undef fseek
#define fseek GPOS_BANNED_API(fseek)
#endif

#ifndef ALLOW_get_nprocs_conf
#undef get_nprocs_conf
#define get_nprocs_conf GPOS_BANNED_API(get_nprocs_conf)
#endif

#ifndef ALLOW_fsetpos
#undef fsetpos
#define fsetpos GPOS_BANNED_API(fsetpos)
#endif

#ifndef ALLOW_getc
#undef getc
#define getc GPOS_BANNED_API(getc)
#endif

#ifndef ALLOW_fsetpos64
#undef fsetpos64
#define fsetpos64 GPOS_BANNED_API(fsetpos64)
#endif

#ifndef ALLOW_getc_unlocked
#undef getc_unlocked
#define getc_unlocked GPOS_BANNED_API(getc_unlocked)
#endif

#ifndef ALLOW_fstat
#undef fstat
#define fstat GPOS_BANNED_API(fstat)
#endif

#ifndef ALLOW_getcontext
#undef getcontext
#define getcontext GPOS_BANNED_API(getcontext)
#endif

#ifndef ALLOW_walkcontext
#undef walkcontext
#define walkcontext GPOS_BANNED_API(walkcontext)
#endif

#ifndef ALLOW_fstat64
#undef fstat64
#define fstat64 GPOS_BANNED_API(fstat64)
#endif

#ifndef ALLOW_getdate_r
#undef getdate_r
#define getdate_r GPOS_BANNED_API(getdate_r)
#endif

#ifndef ALLOW_ftello
#undef ftello
#define ftello GPOS_BANNED_API(ftello)
#endif

#ifndef ALLOW_getdelim
#undef getdelim
#define getdelim GPOS_BANNED_API(getdelim)
#endif

#ifndef ALLOW_ftello64
#undef ftello64
#define ftello64 GPOS_BANNED_API(ftello64)
#endif

#ifndef ALLOW_getdomainnname
#undef getdomainnname
#define getdomainnname GPOS_BANNED_API(getdomainnname)
#endif

#ifndef ALLOW_ftruncate
#undef ftruncate
#define ftruncate GPOS_BANNED_API(ftruncate)
#endif

#ifndef ALLOW_getegid
#undef getegid
#define getegid GPOS_BANNED_API(getegid)
#endif

#ifndef ALLOW_ftruncate64
#undef ftruncate64
#define ftruncate64 GPOS_BANNED_API(ftruncate64)
#endif

#ifndef ALLOW_getenv
#undef getenv
#define getenv GPOS_BANNED_API(getenv)
#endif

#ifndef ALLOW_ftrylockfile
#undef ftrylockfile
#define ftrylockfile GPOS_BANNED_API(ftrylockfile)
#endif

#ifndef ALLOW_geteuid
#undef geteuid
#define geteuid GPOS_BANNED_API(geteuid)
#endif

#ifndef ALLOW_funlockfile
#undef funlockfile
#define funlockfile GPOS_BANNED_API(funlockfile)
#endif

#ifndef ALLOW_getfsfile
#undef getfsfile
#define getfsfile GPOS_BANNED_API(getfsfile)
#endif

#ifndef ALLOW_fwprintf
#undef fwprintf
#define fwprintf GPOS_BANNED_API(fwprintf)
#endif

#ifndef ALLOW_getgrent_r
#undef getgrent_r
#define getgrent_r GPOS_BANNED_API(getgrent_r)
#endif

#ifndef ALLOW_fwscanf
#undef fwscanf
#define fwscanf GPOS_BANNED_API(fwscanf)
#endif

#ifndef ALLOW_getgrouplist
#undef getgrouplist
#define getgrouplist GPOS_BANNED_API(getgrouplist)
#endif

#ifndef ALLOW_gcvt
#undef gcvt
#define gcvt GPOS_BANNED_API(gcvt)
#endif

#ifndef ALLOW_gethostbyaddr_r
#undef gethostbyaddr_r
#define gethostbyaddr_r GPOS_BANNED_API(gethostbyaddr_r)
#endif

#ifndef ALLOW_get_current_dir_name
#undef get_current_dir_name
#define get_current_dir_name GPOS_BANNED_API(get_current_dir_name)
#endif

#ifndef ALLOW_gethostbyname2_r
#undef gethostbyname2_r
#define gethostbyname2_r GPOS_BANNED_API(gethostbyname2_r)
#endif

#ifndef ALLOW_get_nprocs
#undef get_nprocs
#define get_nprocs GPOS_BANNED_API(get_nprocs)
#endif

#ifndef ALLOW_gethostbyname_r
#undef gethostbyname_r
#define gethostbyname_r GPOS_BANNED_API(gethostbyname_r)
#endif

#ifndef ALLOW_get_phys_pages
#undef get_phys_pages
#define get_phys_pages GPOS_BANNED_API(get_phys_pages)
#endif

#ifndef ALLOW_gethostent
#undef gethostent
#define gethostent GPOS_BANNED_API(gethostent)
#endif

#ifndef ALLOW_getchar
#undef getchar
#define getchar GPOS_BANNED_API(getchar)
#endif

#ifndef ALLOW_gethostid
#undef gethostid
#define gethostid GPOS_BANNED_API(gethostid)
#endif

#ifndef ALLOW_getchar_unlocked
#undef getchar_unlocked
#define getchar_unlocked GPOS_BANNED_API(getchar_unlocked)
#endif

#ifndef ALLOW_gethostname
#undef gethostname
#define gethostname GPOS_BANNED_API(gethostname)
#endif

#ifndef ALLOW_getcwd
#undef getcwd
#define getcwd GPOS_BANNED_API(getcwd)
#endif

#ifndef ALLOW_getlogin
#undef getlogin
#define getlogin GPOS_BANNED_API(getlogin)
#endif

#ifndef ALLOW_getfsent
#undef getfsent
#define getfsent GPOS_BANNED_API(getfsent)
#endif

#ifndef ALLOW_getmntent_r
#undef getmntent_r
#define getmntent_r GPOS_BANNED_API(getmntent_r)
#endif

#ifndef ALLOW_getfsspec
#undef getfsspec
#define getfsspec GPOS_BANNED_API(getfsspec)
#endif

#ifndef ALLOW_getnetbyaddr
#undef getnetbyaddr
#define getnetbyaddr GPOS_BANNED_API(getnetbyaddr)
#endif

#ifndef ALLOW_getgid
#undef getgid
#define getgid GPOS_BANNED_API(getgid)
#endif

#ifndef ALLOW_getnetbyname
#undef getnetbyname
#define getnetbyname GPOS_BANNED_API(getnetbyname)
#endif

#ifndef ALLOW_getgrgid_r
#undef getgrgid_r
#define getgrgid_r GPOS_BANNED_API(getgrgid_r)
#endif

#ifndef ALLOW_getopt
#undef getopt
#define getopt GPOS_BANNED_API(getopt)
#endif

#ifndef ALLOW_getgrnam_r
#undef getgrnam_r
#define getgrnam_r GPOS_BANNED_API(getgrnam_r)
#endif

#ifndef ALLOW_getopt_long_only
#undef getopt_long_only
#define getopt_long_only GPOS_BANNED_API(getopt_long_only)
#endif

#ifndef ALLOW_getgroups
#undef getgroups
#define getgroups GPOS_BANNED_API(getgroups)
#endif

#ifndef ALLOW_getpass
#undef getpass
#define getpass GPOS_BANNED_API(getpass)
#endif

#ifndef ALLOW_getitimer
#undef getitimer
#define getitimer GPOS_BANNED_API(getitimer)
#endif

#ifndef ALLOW_getpeername
#undef getpeername
#define getpeername GPOS_BANNED_API(getpeername)
#endif

#ifndef ALLOW_getline
#undef getline
#define getline GPOS_BANNED_API(getline)
#endif

#ifndef ALLOW_getpgrp
#undef getpgrp
#define getpgrp GPOS_BANNED_API(getpgrp)
#endif

#ifndef ALLOW_getloadavg
#undef getloadavg
#define getloadavg GPOS_BANNED_API(getloadavg)
#endif

#ifndef ALLOW_getpid
#undef getpid
#define getpid GPOS_BANNED_API(getpid)
#endif

#ifndef ALLOW_getnetent
#undef getnetent
#define getnetent GPOS_BANNED_API(getnetent)
#endif

#ifndef ALLOW_getppid
#undef getppid
#define getppid GPOS_BANNED_API(getppid)
#endif

#ifndef ALLOW_getnetgrent_r
#undef getnetgrent_r
#define getnetgrent_r GPOS_BANNED_API(getnetgrent_r)
#endif

#ifndef ALLOW_getprotoent
#undef getprotoent
#define getprotoent GPOS_BANNED_API(getprotoent)
#endif

#ifndef ALLOW_getopt_long
#undef getopt_long
#define getopt_long GPOS_BANNED_API(getopt_long)
#endif

#ifndef ALLOW_getpwent_r
#undef getpwent_r
#define getpwent_r GPOS_BANNED_API(getpwent_r)
#endif

#ifndef ALLOW_getpagesize
#undef getpagesize
#define getpagesize GPOS_BANNED_API(getpagesize)
#endif

#ifndef ALLOW_getrlimit
#undef getrlimit
#define getrlimit GPOS_BANNED_API(getrlimit)
#endif

#ifndef ALLOW_getpgid
#undef getpgid
#define getpgid GPOS_BANNED_API(getpgid)
#endif

#ifndef ALLOW_getrlimit64
#undef getrlimit64
#define getrlimit64 GPOS_BANNED_API(getrlimit64)
#endif

#ifndef ALLOW_getpriority
#undef getpriority
#define getpriority GPOS_BANNED_API(getpriority)
#endif

#ifndef ALLOW_getrusage
#undef getrusage
#define getrusage GPOS_BANNED_API(getrusage)
#endif

#ifndef ALLOW_getprotobyname
#undef getprotobyname
#define getprotobyname GPOS_BANNED_API(getprotobyname)
#endif

#ifndef ALLOW_gets
#undef gets
#define gets GPOS_BANNED_API(gets)
#endif

#ifndef ALLOW_getprotobynumber
#undef getprotobynumber
#define getprotobynumber GPOS_BANNED_API(getprotobynumber)
#endif

#ifndef ALLOW_getservent
#undef getservent
#define getservent GPOS_BANNED_API(getservent)
#endif

#ifndef ALLOW_getpt
#undef getpt
#define getpt GPOS_BANNED_API(getpt)
#endif

#ifndef ALLOW_getsockname
#undef getsockname
#define getsockname GPOS_BANNED_API(getsockname)
#endif

#ifndef ALLOW_getpwnam_r
#undef getpwnam_r
#define getpwnam_r GPOS_BANNED_API(getpwnam_r)
#endif

#ifndef ALLOW_getsockopt
#undef getsockopt
#define getsockopt GPOS_BANNED_API(getsockopt)
#endif

#ifndef ALLOW_getpwuid_r
#undef getpwuid_r
#define getpwuid_r GPOS_BANNED_API(getpwuid_r)
#endif

#ifndef ALLOW_getsubopt
#undef getsubopt
#define getsubopt GPOS_BANNED_API(getsubopt)
#endif

#ifndef ALLOW_getservbyname
#undef getservbyname
#define getservbyname GPOS_BANNED_API(getservbyname)
#endif

#ifndef ALLOW_gettext
#undef gettext
#define gettext GPOS_BANNED_API(gettext)
#endif

#ifndef ALLOW_getservbyport
#undef getservbyport
#define getservbyport GPOS_BANNED_API(getservbyport)
#endif

#ifndef ALLOW_getumask
#undef getumask
#define getumask GPOS_BANNED_API(getumask)
#endif

#ifndef ALLOW_getsid
#undef getsid
#define getsid GPOS_BANNED_API(getsid)
#endif

#ifndef ALLOW_getutent_r
#undef getutent_r
#define getutent_r GPOS_BANNED_API(getutent_r)
#endif

#ifndef ALLOW_gettimeofday
#undef gettimeofday
#define gettimeofday GPOS_BANNED_API(gettimeofday)
#endif

#ifndef ALLOW_getutid_r
#undef getutid_r
#define getutid_r GPOS_BANNED_API(getutid_r)
#endif

#ifndef ALLOW_getuid
#undef getuid
#define getuid GPOS_BANNED_API(getuid)
#endif

#ifndef ALLOW_getutxline
#undef getutxline
#define getutxline GPOS_BANNED_API(getutxline)
#endif

#ifndef ALLOW_getutline_r
#undef getutline_r
#define getutline_r GPOS_BANNED_API(getutline_r)
#endif

#ifndef ALLOW_getw
#undef getw
#define getw GPOS_BANNED_API(getw)
#endif

#ifndef ALLOW_getutmp
#undef getutmp
#define getutmp GPOS_BANNED_API(getutmp)
#endif

#ifndef ALLOW_getwchar
#undef getwchar
#define getwchar GPOS_BANNED_API(getwchar)
#endif

#ifndef ALLOW_getutmpx
#undef getutmpx
#define getutmpx GPOS_BANNED_API(getutmpx)
#endif

#ifndef ALLOW_getwchar_unlocked
#undef getwchar_unlocked
#define getwchar_unlocked GPOS_BANNED_API(getwchar_unlocked)
#endif

#ifndef ALLOW_getutxent
#undef getutxent
#define getutxent GPOS_BANNED_API(getutxent)
#endif

#ifndef ALLOW_getwd
#undef getwd
#define getwd GPOS_BANNED_API(getwd)
#endif

#ifndef ALLOW_getutxid
#undef getutxid
#define getutxid GPOS_BANNED_API(getutxid)
#endif

#ifndef ALLOW_gsignal
#undef gsignal
#define gsignal GPOS_BANNED_API(gsignal)
#endif

#ifndef ALLOW_getwc
#undef getwc
#define getwc GPOS_BANNED_API(getwc)
#endif

#ifndef ALLOW_hcreate_r
#undef hcreate_r
#define hcreate_r GPOS_BANNED_API(hcreate_r)
#endif

#ifndef ALLOW_getwc_unlocked
#undef getwc_unlocked
#define getwc_unlocked GPOS_BANNED_API(getwc_unlocked)
#endif

#ifndef ALLOW_hdestroy_r
#undef hdestroy_r
#define hdestroy_r GPOS_BANNED_API(hdestroy_r)
#endif

#ifndef ALLOW_glob
#undef glob
#define glob GPOS_BANNED_API(glob)
#endif

#ifndef ALLOW_hsearch_r
#undef hsearch_r
#define hsearch_r GPOS_BANNED_API(hsearch_r)
#endif

#ifndef ALLOW_glob64
#undef glob64
#define glob64 GPOS_BANNED_API(glob64)
#endif

#ifndef ALLOW_htonl
#undef htonl
#define htonl GPOS_BANNED_API(htonl)
#endif

#ifndef ALLOW_globfree
#undef globfree
#define globfree GPOS_BANNED_API(globfree)
#endif

#ifndef ALLOW_iconv
#undef iconv
#define iconv GPOS_BANNED_API(iconv)
#endif

#ifndef ALLOW_globfree64
#undef globfree64
#define globfree64 GPOS_BANNED_API(globfree64)
#endif

#ifndef ALLOW_if_freenameindex
#undef if_freenameindex
#define if_freenameindex GPOS_BANNED_API(if_freenameindex)
#endif

#ifndef ALLOW_gmtime_r
#undef gmtime_r
#define gmtime_r GPOS_BANNED_API(gmtime_r)
#endif

#ifndef ALLOW_if_nameindex
#undef if_nameindex
#define if_nameindex GPOS_BANNED_API(if_nameindex)
#endif

#ifndef ALLOW_grantpt
#undef grantpt
#define grantpt GPOS_BANNED_API(grantpt)
#endif

#ifndef ALLOW_ilogb
#undef ilogb
#define ilogb GPOS_BANNED_API(ilogb)
#endif

#ifndef ALLOW_gtty
#undef gtty
#define gtty GPOS_BANNED_API(gtty)
#endif

#ifndef ALLOW_ilogbf
#undef ilogbf
#define ilogbf GPOS_BANNED_API(ilogbf)
#endif

#ifndef ALLOW_hasmntopt
#undef hasmntopt
#define hasmntopt GPOS_BANNED_API(hasmntopt)
#endif

#ifndef ALLOW_ilogbl
#undef ilogbl
#define ilogbl GPOS_BANNED_API(ilogbl)
#endif

#ifndef ALLOW_htons
#undef htons
#define htons GPOS_BANNED_API(htons)
#endif

#ifndef ALLOW_imaxabs
#undef imaxabs
#define imaxabs GPOS_BANNED_API(imaxabs)
#endif

#ifndef ALLOW_hypot
#undef hypot
#define hypot GPOS_BANNED_API(hypot)
#endif

#ifndef ALLOW_inet_aton
#undef inet_aton
#define inet_aton GPOS_BANNED_API(inet_aton)
#endif

#ifndef ALLOW_hypotf
#undef hypotf
#define hypotf GPOS_BANNED_API(hypotf)
#endif

#ifndef ALLOW_inet_lnaof
#undef inet_lnaof
#define inet_lnaof GPOS_BANNED_API(inet_lnaof)
#endif

#ifndef ALLOW_hypotl
#undef hypotl
#define hypotl GPOS_BANNED_API(hypotl)
#endif

#ifndef ALLOW_inet_netof
#undef inet_netof
#define inet_netof GPOS_BANNED_API(inet_netof)
#endif

#ifndef ALLOW_iconv_close
#undef iconv_close
#define iconv_close GPOS_BANNED_API(iconv_close)
#endif

#ifndef ALLOW_inet_network
#undef inet_network
#define inet_network GPOS_BANNED_API(inet_network)
#endif

#ifndef ALLOW_iconv_open
#undef iconv_open
#define iconv_open GPOS_BANNED_API(iconv_open)
#endif

#ifndef ALLOW_inet_ntoa
#undef inet_ntoa
#define inet_ntoa GPOS_BANNED_API(inet_ntoa)
#endif

#ifndef ALLOW_if_indextoname
#undef if_indextoname
#define if_indextoname GPOS_BANNED_API(if_indextoname)
#endif

#ifndef ALLOW_innetgr
#undef innetgr
#define innetgr GPOS_BANNED_API(innetgr)
#endif

#ifndef ALLOW_if_nametoindex
#undef if_nametoindex
#define if_nametoindex GPOS_BANNED_API(if_nametoindex)
#endif

#ifndef ALLOW_IFTODT
#undef IFTODT
#define IFTODT GPOS_BANNED_API(IFTODT)
#endif

#ifndef ALLOW_ioctl
#undef ioctl
#define ioctl GPOS_BANNED_API(ioctl)
#endif

#ifndef ALLOW_imaxdiv
#undef imaxdiv
#define imaxdiv GPOS_BANNED_API(imaxdiv)
#endif

#ifndef ALLOW_isalnum
#undef isalnum
#define isalnum GPOS_BANNED_API(isalnum)
#endif

#ifndef ALLOW_index
#undef index
#define index GPOS_BANNED_API(index)
#endif

#ifndef ALLOW_isascii
#undef isascii
#define isascii GPOS_BANNED_API(isascii)
#endif

#ifndef ALLOW_inet_addr
#undef inet_addr
#define inet_addr GPOS_BANNED_API(inet_addr)
#endif

#ifndef ALLOW_iscntrl
#undef iscntrl
#define iscntrl GPOS_BANNED_API(iscntrl)
#endif

#ifndef ALLOW_inet_makeaddr
#undef inet_makeaddr
#define inet_makeaddr GPOS_BANNED_API(inet_makeaddr)
#endif

#ifndef ALLOW_isdigit
#undef isdigit
#define isdigit GPOS_BANNED_API(isdigit)
#endif

#ifndef ALLOW_inet_ntop
#undef inet_ntop
#define inet_ntop GPOS_BANNED_API(inet_ntop)
#endif

#ifndef ALLOW_isfinite
#undef isfinite
#define isfinite GPOS_BANNED_API(isfinite)
#endif

#ifndef ALLOW_inet_pton
#undef inet_pton
#define inet_pton GPOS_BANNED_API(inet_pton)
#endif

#ifndef ALLOW_isinf
#undef isinf
#define isinf GPOS_BANNED_API(isinf)
#endif

#ifndef ALLOW_initgroups
#undef initgroups
#define initgroups GPOS_BANNED_API(initgroups)
#endif

#ifndef ALLOW_isinff
#undef isinff
#define isinff GPOS_BANNED_API(isinff)
#endif

#ifndef ALLOW_initstate_r
#undef initstate_r
#define initstate_r GPOS_BANNED_API(initstate_r)
#endif

#ifndef ALLOW_isinfl
#undef isinfl
#define isinfl GPOS_BANNED_API(isinfl)
#endif

#ifndef ALLOW_isalpha
#undef isalpha
#define isalpha GPOS_BANNED_API(isalpha)
#endif

#ifndef ALLOW_isless
#undef isless
#define isless GPOS_BANNED_API(isless)
#endif

#ifndef ALLOW_isatty
#undef isatty
#define isatty GPOS_BANNED_API(isatty)
#endif

#ifndef ALLOW_islessequal
#undef islessequal
#define islessequal GPOS_BANNED_API(islessequal)
#endif

#ifndef ALLOW_isblank
#undef isblank
#define isblank GPOS_BANNED_API(isblank)
#endif

#ifndef ALLOW_islessgreater
#undef islessgreater
#define islessgreater GPOS_BANNED_API(islessgreater)
#endif

#ifndef ALLOW_isgraph
#undef isgraph
#define isgraph GPOS_BANNED_API(isgraph)
#endif

#ifndef ALLOW_islower
#undef islower
#define islower GPOS_BANNED_API(islower)
#endif

#ifndef ALLOW_isgreater
#undef isgreater
#define isgreater GPOS_BANNED_API(isgreater)
#endif

#ifndef ALLOW_isnan
#undef isnan
#define isnan GPOS_BANNED_API(isnan)
#endif

#ifndef ALLOW_isgreaterequal
#undef isgreaterequal
#define isgreaterequal GPOS_BANNED_API(isgreaterequal)
#endif

#ifndef ALLOW_isnanf
#undef isnanf
#define isnanf GPOS_BANNED_API(isnanf)
#endif

#ifndef ALLOW_ispunct
#undef ispunct
#define ispunct GPOS_BANNED_API(ispunct)
#endif

#ifndef ALLOW_isnanl
#undef isnanl
#define isnanl GPOS_BANNED_API(isnanl)
#endif

#ifndef ALLOW_isspace
#undef isspace
#define isspace GPOS_BANNED_API(isspace)
#endif

#ifndef ALLOW_isnormal
#undef isnormal
#define isnormal GPOS_BANNED_API(isnormal)
#endif

#ifndef ALLOW_isunordered
#undef isunordered
#define isunordered GPOS_BANNED_API(isunordered)
#endif

#ifndef ALLOW_isprint
#undef isprint
#define isprint GPOS_BANNED_API(isprint)
#endif

#ifndef ALLOW_isupper
#undef isupper
#define isupper GPOS_BANNED_API(isupper)
#endif

#ifndef ALLOW_iswalpha
#undef iswalpha
#define iswalpha GPOS_BANNED_API(iswalpha)
#endif

#ifndef ALLOW_iswalnum
#undef iswalnum
#define iswalnum GPOS_BANNED_API(iswalnum)
#endif

#ifndef ALLOW_iswblank
#undef iswblank
#define iswblank GPOS_BANNED_API(iswblank)
#endif

#ifndef ALLOW_iswcntrl
#undef iswcntrl
#define iswcntrl GPOS_BANNED_API(iswcntrl)
#endif

#ifndef ALLOW_iswgraph
#undef iswgraph
#define iswgraph GPOS_BANNED_API(iswgraph)
#endif

#ifndef ALLOW_iswctype
#undef iswctype
#define iswctype GPOS_BANNED_API(iswctype)
#endif

#ifndef ALLOW_iswpunct
#undef iswpunct
#define iswpunct GPOS_BANNED_API(iswpunct)
#endif

#ifndef ALLOW_iswdigit
#undef iswdigit
#define iswdigit GPOS_BANNED_API(iswdigit)
#endif

#ifndef ALLOW_iswspace
#undef iswspace
#define iswspace GPOS_BANNED_API(iswspace)
#endif

#ifndef ALLOW_iswlower
#undef iswlower
#define iswlower GPOS_BANNED_API(iswlower)
#endif

#ifndef ALLOW_iswupper
#undef iswupper
#define iswupper GPOS_BANNED_API(iswupper)
#endif

#ifndef ALLOW_iswprint
#undef iswprint
#define iswprint GPOS_BANNED_API(iswprint)
#endif

#ifndef ALLOW_isxdigit
#undef isxdigit
#define isxdigit GPOS_BANNED_API(isxdigit)
#endif

#ifndef ALLOW_iswxdigit
#undef iswxdigit
#define iswxdigit GPOS_BANNED_API(iswxdigit)
#endif

#ifndef ALLOW_j1
#undef j1
#define j1 GPOS_BANNED_API(j1)
#endif

#ifndef ALLOW_j0
#undef j0
#define j0 GPOS_BANNED_API(j0)
#endif

#ifndef ALLOW_j1f
#undef j1f
#define j1f GPOS_BANNED_API(j1f)
#endif

#ifndef ALLOW_j0f
#undef j0f
#define j0f GPOS_BANNED_API(j0f)
#endif

#ifndef ALLOW_j1l
#undef j1l
#define j1l GPOS_BANNED_API(j1l)
#endif

#ifndef ALLOW_j0l
#undef j0l
#define j0l GPOS_BANNED_API(j0l)
#endif

#ifndef ALLOW_killpg
#undef killpg
#define killpg GPOS_BANNED_API(killpg)
#endif

#ifndef ALLOW_jn
#undef jn
#define jn GPOS_BANNED_API(jn)
#endif

#ifndef ALLOW_l64a
#undef l64a
#define l64a GPOS_BANNED_API(l64a)
#endif

#ifndef ALLOW_jnf
#undef jnf
#define jnf GPOS_BANNED_API(jnf)
#endif

#ifndef ALLOW_ldexp
#undef ldexp
#define ldexp GPOS_BANNED_API(ldexp)
#endif

#ifndef ALLOW_jnl
#undef jnl
#define jnl GPOS_BANNED_API(jnl)
#endif

#ifndef ALLOW_ldexpf
#undef ldexpf
#define ldexpf GPOS_BANNED_API(ldexpf)
#endif

#ifndef ALLOW_jrand48_r
#undef jrand48_r
#define jrand48_r GPOS_BANNED_API(jrand48_r)
#endif

#ifndef ALLOW_ldexpl
#undef ldexpl
#define ldexpl GPOS_BANNED_API(ldexpl)
#endif

#ifndef ALLOW_kill
#undef kill
#define kill GPOS_BANNED_API(kill)
#endif

#ifndef ALLOW_ldiv
#undef ldiv
#define ldiv GPOS_BANNED_API(ldiv)
#endif

#ifndef ALLOW_labs
#undef labs
#define labs GPOS_BANNED_API(labs)
#endif

#ifndef ALLOW_lfind
#undef lfind
#define lfind GPOS_BANNED_API(lfind)
#endif

#ifndef ALLOW_lcong48_r
#undef lcong48_r
#define lcong48_r GPOS_BANNED_API(lcong48_r)
#endif

#ifndef ALLOW_lio_listio
#undef lio_listio
#define lio_listio GPOS_BANNED_API(lio_listio)
#endif

#ifndef ALLOW_lgamma_r
#undef lgamma_r
#define lgamma_r GPOS_BANNED_API(lgamma_r)
#endif

#ifndef ALLOW_lio_listio64
#undef lio_listio64
#define lio_listio64 GPOS_BANNED_API(lio_listio64)
#endif

#ifndef ALLOW_lgammaf_r
#undef lgammaf_r
#define lgammaf_r GPOS_BANNED_API(lgammaf_r)
#endif

#ifndef ALLOW_listen
#undef listen
#define listen GPOS_BANNED_API(listen)
#endif

#ifndef ALLOW_lgammal_r
#undef lgammal_r
#define lgammal_r GPOS_BANNED_API(lgammal_r)
#endif

#ifndef ALLOW_lldiv
#undef lldiv
#define lldiv GPOS_BANNED_API(lldiv)
#endif

#ifndef ALLOW_link
#undef link
#define link GPOS_BANNED_API(link)
#endif

#ifndef ALLOW_llrint
#undef llrint
#define llrint GPOS_BANNED_API(llrint)
#endif

#ifndef ALLOW_llabs
#undef llabs
#define llabs GPOS_BANNED_API(llabs)
#endif

#ifndef ALLOW_llrintf
#undef llrintf
#define llrintf GPOS_BANNED_API(llrintf)
#endif

#ifndef ALLOW_llround
#undef llround
#define llround GPOS_BANNED_API(llround)
#endif

#ifndef ALLOW_llrintl
#undef llrintl
#define llrintl GPOS_BANNED_API(llrintl)
#endif

#ifndef ALLOW_llroundf
#undef llroundf
#define llroundf GPOS_BANNED_API(llroundf)
#endif

#ifndef ALLOW_localtime_r
#undef localtime_r
#define localtime_r GPOS_BANNED_API(localtime_r)
#endif

#ifndef ALLOW_llroundl
#undef llroundl
#define llroundl GPOS_BANNED_API(llroundl)
#endif

#ifndef ALLOW_log10
#undef log10
#define log10 GPOS_BANNED_API(log10)
#endif

#ifndef ALLOW_localeconv
#undef localeconv
#define localeconv GPOS_BANNED_API(localeconv)
#endif

#ifndef ALLOW_log10f
#undef log10f
#define log10f GPOS_BANNED_API(log10f)
#endif

#ifndef ALLOW_log
#undef log
#define log GPOS_BANNED_API(log)
#endif

#ifndef ALLOW_log10l
#undef log10l
#define log10l GPOS_BANNED_API(log10l)
#endif

#ifndef ALLOW_log2
#undef log2
#define log2 GPOS_BANNED_API(log2)
#endif

#ifndef ALLOW_log1p
#undef log1p
#define log1p GPOS_BANNED_API(log1p)
#endif

#ifndef ALLOW_log2f
#undef log2f
#define log2f GPOS_BANNED_API(log2f)
#endif

#ifndef ALLOW_log1pf
#undef log1pf
#define log1pf GPOS_BANNED_API(log1pf)
#endif

#ifndef ALLOW_log2l
#undef log2l
#define log2l GPOS_BANNED_API(log2l)
#endif

#ifndef ALLOW_log1pl
#undef log1pl
#define log1pl GPOS_BANNED_API(log1pl)
#endif

#ifndef ALLOW_logb
#undef logb
#define logb GPOS_BANNED_API(logb)
#endif

#ifndef ALLOW_login
#undef login
#define login GPOS_BANNED_API(login)
#endif

#ifndef ALLOW_logbf
#undef logbf
#define logbf GPOS_BANNED_API(logbf)
#endif

#ifndef ALLOW_login_tty
#undef login_tty
#define login_tty GPOS_BANNED_API(login_tty)
#endif

#ifndef ALLOW_logbl
#undef logbl
#define logbl GPOS_BANNED_API(logbl)
#endif

#ifndef ALLOW_longjmp
#undef longjmp
#define longjmp GPOS_BANNED_API(longjmp)
#endif

#ifndef ALLOW_logf
#undef logf
#define logf GPOS_BANNED_API(logf)
#endif

#ifndef ALLOW_lrint
#undef lrint
#define lrint GPOS_BANNED_API(lrint)
#endif

#ifndef ALLOW_logl
#undef logl
#define logl GPOS_BANNED_API(logl)
#endif

#ifndef ALLOW_lrintf
#undef lrintf
#define lrintf GPOS_BANNED_API(lrintf)
#endif

#ifndef ALLOW_logout
#undef logout
#define logout GPOS_BANNED_API(logout)
#endif

#ifndef ALLOW_lrintl
#undef lrintl
#define lrintl GPOS_BANNED_API(lrintl)
#endif

#ifndef ALLOW_logwtmp
#undef logwtmp
#define logwtmp GPOS_BANNED_API(logwtmp)
#endif

#ifndef ALLOW_lutimes
#undef lutimes
#define lutimes GPOS_BANNED_API(lutimes)
#endif

#ifndef ALLOW_lrand48_r
#undef lrand48_r
#define lrand48_r GPOS_BANNED_API(lrand48_r)
#endif

#ifndef ALLOW_madvise
#undef madvise
#define madvise GPOS_BANNED_API(madvise)
#endif

#ifndef ALLOW_lround
#undef lround
#define lround GPOS_BANNED_API(lround)
#endif

#ifndef ALLOW_lroundf
#undef lroundf
#define lroundf GPOS_BANNED_API(lroundf)
#endif

#ifndef ALLOW_makecontext
#undef makecontext
#define makecontext GPOS_BANNED_API(makecontext)
#endif

#ifndef ALLOW_lroundl
#undef lroundl
#define lroundl GPOS_BANNED_API(lroundl)
#endif

#ifndef ALLOW_mallopt
#undef mallopt
#define mallopt GPOS_BANNED_API(mallopt)
#endif

#ifndef ALLOW_lsearch
#undef lsearch
#define lsearch GPOS_BANNED_API(lsearch)
#endif

#ifndef ALLOW_matherr
#undef matherr
#define matherr GPOS_BANNED_API(matherr)
#endif

#ifndef ALLOW_lseek
#undef lseek
#define lseek GPOS_BANNED_API(lseek)
#endif

#ifndef ALLOW_mcheck
#undef mcheck
#define mcheck GPOS_BANNED_API(mcheck)
#endif

#ifndef ALLOW_lseek64
#undef lseek64
#define lseek64 GPOS_BANNED_API(lseek64)
#endif

#ifndef ALLOW_memcmp
#undef memcmp
#define memcmp GPOS_BANNED_API(memcmp)
#endif

#ifndef ALLOW_lstat
#undef lstat
#define lstat GPOS_BANNED_API(lstat)
#endif

#ifndef ALLOW_memcpy
#undef memcpy
#define memcpy GPOS_BANNED_API(memcpy)
#endif

#ifndef ALLOW_lstat64
#undef lstat64
#define lstat64 GPOS_BANNED_API(lstat64)
#endif

#ifndef ALLOW_mempcpy
#undef mempcpy
#define mempcpy GPOS_BANNED_API(mempcpy)
#endif

#ifndef ALLOW_mallinfo
#undef mallinfo
#define mallinfo GPOS_BANNED_API(mallinfo)
#endif

#ifndef ALLOW_memset
#undef memset
#define memset GPOS_BANNED_API(memset)
#endif

#ifndef ALLOW_malloc
#undef malloc
#define malloc GPOS_BANNED_API(malloc)
#endif

#ifndef ALLOW_mkdir
#undef mkdir
#define mkdir GPOS_BANNED_API(mkdir)
#endif

#ifndef ALLOW_mblen
#undef mblen
#define mblen GPOS_BANNED_API(mblen)
#endif

#ifndef ALLOW_mknod
#undef mknod
#define mknod GPOS_BANNED_API(mknod)
#endif

#ifndef ALLOW_mbrlen
#undef mbrlen
#define mbrlen GPOS_BANNED_API(mbrlen)
#endif

#ifndef ALLOW_mkstemp
#undef mkstemp
#define mkstemp GPOS_BANNED_API(mkstemp)
#endif

#ifndef ALLOW_mbrtowc
#undef mbrtowc
#define mbrtowc GPOS_BANNED_API(mbrtowc)
#endif

#ifndef ALLOW_mktime
#undef mktime
#define mktime GPOS_BANNED_API(mktime)
#endif

#ifndef ALLOW_mbsinit
#undef mbsinit
#define mbsinit GPOS_BANNED_API(mbsinit)
#endif

#ifndef ALLOW_mlockall
#undef mlockall
#define mlockall GPOS_BANNED_API(mlockall)
#endif

#ifndef ALLOW_mbsnrtowcs
#undef mbsnrtowcs
#define mbsnrtowcs GPOS_BANNED_API(mbsnrtowcs)
#endif

#ifndef ALLOW_mmap
#undef mmap
#define mmap GPOS_BANNED_API(mmap)
#endif

#ifndef ALLOW_mbsrtowcs
#undef mbsrtowcs
#define mbsrtowcs GPOS_BANNED_API(mbsrtowcs)
#endif

#ifndef ALLOW_mmap64
#undef mmap64
#define mmap64 GPOS_BANNED_API(mmap64)
#endif

#ifndef ALLOW_mbstowcs
#undef mbstowcs
#define mbstowcs GPOS_BANNED_API(mbstowcs)
#endif

#ifndef ALLOW_mount
#undef mount
#define mount GPOS_BANNED_API(mount)
#endif

#ifndef ALLOW_mbtowc
#undef mbtowc
#define mbtowc GPOS_BANNED_API(mbtowc)
#endif

#ifndef ALLOW_mprobe
#undef mprobe
#define mprobe GPOS_BANNED_API(mprobe)
#endif

#ifndef ALLOW_memalign
#undef memalign
#define memalign GPOS_BANNED_API(memalign)
#endif

#ifndef ALLOW_mrand48_r
#undef mrand48_r
#define mrand48_r GPOS_BANNED_API(mrand48_r)
#endif

#ifndef ALLOW_memccpy
#undef memccpy
#define memccpy GPOS_BANNED_API(memccpy)
#endif

#ifndef ALLOW_munlock
#undef munlock
#define munlock GPOS_BANNED_API(munlock)
#endif

#ifndef ALLOW_memchr
#undef memchr
#define memchr GPOS_BANNED_API(memchr)
#endif

#ifndef ALLOW_muntrace
#undef muntrace
#define muntrace GPOS_BANNED_API(muntrace)
#endif

#ifndef ALLOW_memfrob
#undef memfrob
#define memfrob GPOS_BANNED_API(memfrob)
#endif

#ifndef ALLOW_nan
#undef nan
#define nan GPOS_BANNED_API(nan)
#endif

#ifndef ALLOW_memmem
#undef memmem
#define memmem GPOS_BANNED_API(memmem)
#endif

#ifndef ALLOW_nanf
#undef nanf
#define nanf GPOS_BANNED_API(nanf)
#endif

#ifndef ALLOW_memmove
#undef memmove
#define memmove GPOS_BANNED_API(memmove)
#endif

#ifndef ALLOW_nanl
#undef nanl
#define nanl GPOS_BANNED_API(nanl)
#endif

#ifndef ALLOW_memrchr
#undef memrchr
#define memrchr GPOS_BANNED_API(memrchr)
#endif

#ifndef ALLOW_nanosleep
#undef nanosleep
#define nanosleep GPOS_BANNED_API(nanosleep)
#endif

#ifndef ALLOW_mkdtemp
#undef mkdtemp
#define mkdtemp GPOS_BANNED_API(mkdtemp)
#endif

#ifndef ALLOW_nextafter
#undef nextafter
#define nextafter GPOS_BANNED_API(nextafter)
#endif

#ifndef ALLOW_mkfifo
#undef mkfifo
#define mkfifo GPOS_BANNED_API(mkfifo)
#endif

#ifndef ALLOW_nextafterf
#undef nextafterf
#define nextafterf GPOS_BANNED_API(nextafterf)
#endif

#ifndef ALLOW_mktemp
#undef mktemp
#define mktemp GPOS_BANNED_API(mktemp)
#endif

#ifndef ALLOW_nextafterl
#undef nextafterl
#define nextafterl GPOS_BANNED_API(nextafterl)
#endif

#ifndef ALLOW_mlock
#undef mlock
#define mlock GPOS_BANNED_API(mlock)
#endif

#ifndef ALLOW_nftw
#undef nftw
#define nftw GPOS_BANNED_API(nftw)
#endif

#ifndef ALLOW_modf
#undef modf
#define modf GPOS_BANNED_API(modf)
#endif

#ifndef ALLOW_nftw64
#undef nftw64
#define nftw64 GPOS_BANNED_API(nftw64)
#endif

#ifndef ALLOW_modff
#undef modff
#define modff GPOS_BANNED_API(modff)
#endif

#ifndef ALLOW_ngettext
#undef ngettext
#define ngettext GPOS_BANNED_API(ngettext)
#endif

#ifndef ALLOW_modfl
#undef modfl
#define modfl GPOS_BANNED_API(modfl)
#endif

#ifndef ALLOW_nice
#undef nice
#define nice GPOS_BANNED_API(nice)
#endif

#ifndef ALLOW_mremap
#undef mremap
#define mremap GPOS_BANNED_API(mremap)
#endif

#ifndef ALLOW_nl_langinfo
#undef nl_langinfo
#define nl_langinfo GPOS_BANNED_API(nl_langinfo)
#endif

#ifndef ALLOW_msync
#undef msync
#define msync GPOS_BANNED_API(msync)
#endif

#ifndef ALLOW_notfound
#undef notfound
#define notfound GPOS_BANNED_API(notfound)
#endif

#ifndef ALLOW_mtrace
#undef mtrace
#define mtrace GPOS_BANNED_API(mtrace)
#endif

#ifndef ALLOW_ntohl
#undef ntohl
#define ntohl GPOS_BANNED_API(ntohl)
#endif

#ifndef ALLOW_munlockall
#undef munlockall
#define munlockall GPOS_BANNED_API(munlockall)
#endif

#ifndef ALLOW_ntp_adjtime
#undef ntp_adjtime
#define ntp_adjtime GPOS_BANNED_API(ntp_adjtime)
#endif

#ifndef ALLOW_munmap
#undef munmap
#define munmap GPOS_BANNED_API(munmap)
#endif

#ifndef ALLOW_obstack_1grow_fast
#undef obstack_1grow_fast
#define obstack_1grow_fast GPOS_BANNED_API(obstack_1grow_fast)
#endif

#ifndef ALLOW_nearbyint
#undef nearbyint
#define nearbyint GPOS_BANNED_API(nearbyint)
#endif

#ifndef ALLOW_obstack_alloc
#undef obstack_alloc
#define obstack_alloc GPOS_BANNED_API(obstack_alloc)
#endif

#ifndef ALLOW_nearbyintf
#undef nearbyintf
#define nearbyintf GPOS_BANNED_API(nearbyintf)
#endif

#ifndef ALLOW_obstack_base
#undef obstack_base
#define obstack_base GPOS_BANNED_API(obstack_base)
#endif

#ifndef ALLOW_nearbyintl
#undef nearbyintl
#define nearbyintl GPOS_BANNED_API(nearbyintl)
#endif

#ifndef ALLOW_obstack_blank_fast
#undef obstack_blank_fast
#define obstack_blank_fast GPOS_BANNED_API(obstack_blank_fast)
#endif

#ifndef ALLOW_nexttoward
#undef nexttoward
#define nexttoward GPOS_BANNED_API(nexttoward)
#endif

#ifndef ALLOW_obstack_chunk_alloc
#undef obstack_chunk_alloc
#define obstack_chunk_alloc GPOS_BANNED_API(obstack_chunk_alloc)
#endif

#ifndef ALLOW_nexttowardf
#undef nexttowardf
#define nexttowardf GPOS_BANNED_API(nexttowardf)
#endif

#ifndef ALLOW_obstack_chunk_size
#undef obstack_chunk_size
#define obstack_chunk_size GPOS_BANNED_API(obstack_chunk_size)
#endif

#ifndef ALLOW_nexttowardl
#undef nexttowardl
#define nexttowardl GPOS_BANNED_API(nexttowardl)
#endif

#ifndef ALLOW_obstack_copy
#undef obstack_copy
#define obstack_copy GPOS_BANNED_API(obstack_copy)
#endif

#ifndef ALLOW_nrand48_r
#undef nrand48_r
#define nrand48_r GPOS_BANNED_API(nrand48_r)
#endif

#ifndef ALLOW_obstack_copy0
#undef obstack_copy0
#define obstack_copy0 GPOS_BANNED_API(obstack_copy0)
#endif

#ifndef ALLOW_ntohs
#undef ntohs
#define ntohs GPOS_BANNED_API(ntohs)
#endif

#ifndef ALLOW_obstack_finish
#undef obstack_finish
#define obstack_finish GPOS_BANNED_API(obstack_finish)
#endif

#ifndef ALLOW_ntp_gettime
#undef ntp_gettime
#define ntp_gettime GPOS_BANNED_API(ntp_gettime)
#endif

#ifndef ALLOW_obstack_grow
#undef obstack_grow
#define obstack_grow GPOS_BANNED_API(obstack_grow)
#endif

#ifndef ALLOW_obstack_1grow
#undef obstack_1grow
#define obstack_1grow GPOS_BANNED_API(obstack_1grow)
#endif

#ifndef ALLOW_obstack_grow0
#undef obstack_grow0
#define obstack_grow0 GPOS_BANNED_API(obstack_grow0)
#endif

#ifndef ALLOW_obstack_alignment_mask
#undef obstack_alignment_mask
#define obstack_alignment_mask GPOS_BANNED_API(obstack_alignment_mask)
#endif

#ifndef ALLOW_obstack_int_grow
#undef obstack_int_grow
#define obstack_int_grow GPOS_BANNED_API(obstack_int_grow)
#endif

#ifndef ALLOW_obstack_blank
#undef obstack_blank
#define obstack_blank GPOS_BANNED_API(obstack_blank)
#endif

#ifndef ALLOW_obstack_object_size
#undef obstack_object_size
#define obstack_object_size GPOS_BANNED_API(obstack_object_size)
#endif

#ifndef ALLOW_obstack_chunk_free
#undef obstack_chunk_free
#define obstack_chunk_free GPOS_BANNED_API(obstack_chunk_free)
#endif

#ifndef ALLOW_obstack_object_size
#undef obstack_object_size
#define obstack_object_size GPOS_BANNED_API(obstack_object_size)
#endif

#ifndef ALLOW_obstack_free
#undef obstack_free
#define obstack_free GPOS_BANNED_API(obstack_free)
#endif

#ifndef ALLOW_obstack_printf
#undef obstack_printf
#define obstack_printf GPOS_BANNED_API(obstack_printf)
#endif

#ifndef ALLOW_obstack_init
#undef obstack_init
#define obstack_init GPOS_BANNED_API(obstack_init)
#endif

#ifndef ALLOW_obstack_ptr_grow_fast
#undef obstack_ptr_grow_fast
#define obstack_ptr_grow_fast GPOS_BANNED_API(obstack_ptr_grow_fast)
#endif

#ifndef ALLOW_obstack_int_grow_fast
#undef obstack_int_grow_fast
#define obstack_int_grow_fast GPOS_BANNED_API(obstack_int_grow_fast)
#endif

#ifndef ALLOW_obstack_room
#undef obstack_room
#define obstack_room GPOS_BANNED_API(obstack_room)
#endif

#ifndef ALLOW_obstack_next_free
#undef obstack_next_free
#define obstack_next_free GPOS_BANNED_API(obstack_next_free)
#endif

#ifndef ALLOW_obstack_vprintf
#undef obstack_vprintf
#define obstack_vprintf GPOS_BANNED_API(obstack_vprintf)
#endif

#ifndef ALLOW_obstack_ptr_grow
#undef obstack_ptr_grow
#define obstack_ptr_grow GPOS_BANNED_API(obstack_ptr_grow)
#endif

#ifndef ALLOW_open_obstack_stream
#undef open_obstack_stream
#define open_obstack_stream GPOS_BANNED_API(open_obstack_stream)
#endif

#ifndef ALLOW_offsetof
#undef offsetof
#define offsetof GPOS_BANNED_API(offsetof)
#endif

#ifndef ALLOW_opendir
#undef opendir
#define opendir GPOS_BANNED_API(opendir)
#endif

#ifndef ALLOW_on_exit
#undef on_exit
#define on_exit GPOS_BANNED_API(on_exit)
#endif

#ifndef ALLOW_openpty
#undef openpty
#define openpty GPOS_BANNED_API(openpty)
#endif

#ifndef ALLOW_open
#undef open
#define open GPOS_BANNED_API(open)
#endif

#ifndef ALLOW_parse_printf_format
#undef parse_printf_format
#define parse_printf_format GPOS_BANNED_API(parse_printf_format)
#endif

#ifndef ALLOW_open64
#undef open64
#define open64 GPOS_BANNED_API(open64)
#endif

#ifndef ALLOW_pathconf
#undef pathconf
#define pathconf GPOS_BANNED_API(pathconf)
#endif

#ifndef ALLOW_open_memstream
#undef open_memstream
#define open_memstream GPOS_BANNED_API(open_memstream)
#endif

#ifndef ALLOW_pow10
#undef pow10
#define pow10 GPOS_BANNED_API(pow10)
#endif

#ifndef ALLOW_openlog
#undef openlog
#define openlog GPOS_BANNED_API(openlog)
#endif

#ifndef ALLOW_pow10f
#undef pow10f
#define pow10f GPOS_BANNED_API(pow10f)
#endif

#ifndef ALLOW_pause
#undef pause
#define pause GPOS_BANNED_API(pause)
#endif

#ifndef ALLOW_pow10l
#undef pow10l
#define pow10l GPOS_BANNED_API(pow10l)
#endif

#ifndef ALLOW_pclose
#undef pclose
#define pclose GPOS_BANNED_API(pclose)
#endif

#ifndef ALLOW_printf
#undef printf
#define printf GPOS_BANNED_API(printf)
#endif

#ifndef ALLOW_perror
#undef perror
#define perror GPOS_BANNED_API(perror)
#endif

#ifndef ALLOW_printf_size
#undef printf_size
#define printf_size GPOS_BANNED_API(printf_size)
#endif

#ifndef ALLOW_pipe
#undef pipe
#define pipe GPOS_BANNED_API(pipe)
#endif

#ifndef ALLOW_ptsname_r
#undef ptsname_r
#define ptsname_r GPOS_BANNED_API(ptsname_r)
#endif

#ifndef ALLOW_popen
#undef popen
#define popen GPOS_BANNED_API(popen)
#endif

#ifndef ALLOW_putchar
#undef putchar
#define putchar GPOS_BANNED_API(putchar)
#endif

#ifndef ALLOW_posix_memalign
#undef posix_memalign
#define posix_memalign GPOS_BANNED_API(posix_memalign)
#endif

#ifndef ALLOW_putchar_unlocked
#undef putchar_unlocked
#define putchar_unlocked GPOS_BANNED_API(putchar_unlocked)
#endif

#ifndef ALLOW_pow
#undef pow
#define pow GPOS_BANNED_API(pow)
#endif

#ifndef ALLOW_putpwent
#undef putpwent
#define putpwent GPOS_BANNED_API(putpwent)
#endif

#ifndef ALLOW_powf
#undef powf
#define powf GPOS_BANNED_API(powf)
#endif

#ifndef ALLOW_putwc
#undef putwc
#define putwc GPOS_BANNED_API(putwc)
#endif

#ifndef ALLOW_powl
#undef powl
#define powl GPOS_BANNED_API(powl)
#endif

#ifndef ALLOW_putwc_unlocked
#undef putwc_unlocked
#define putwc_unlocked GPOS_BANNED_API(putwc_unlocked)
#endif

#ifndef ALLOW_pread
#undef pread
#define pread GPOS_BANNED_API(pread)
#endif

#ifndef ALLOW_pwrite
#undef pwrite
#define pwrite GPOS_BANNED_API(pwrite)
#endif

#ifndef ALLOW_pread64
#undef pread64
#define pread64 GPOS_BANNED_API(pread64)
#endif

#ifndef ALLOW_pwrite64
#undef pwrite64
#define pwrite64 GPOS_BANNED_API(pwrite64)
#endif

#ifndef ALLOW_printf_size_info
#undef printf_size_info
#define printf_size_info GPOS_BANNED_API(printf_size_info)
#endif

#ifndef ALLOW_qfcvt_r
#undef qfcvt_r
#define qfcvt_r GPOS_BANNED_API(qfcvt_r)
#endif

#ifndef ALLOW_psignal
#undef psignal
#define psignal GPOS_BANNED_API(psignal)
#endif

#ifndef ALLOW_qgcvt
#undef qgcvt
#define qgcvt GPOS_BANNED_API(qgcvt)
#endif

#ifndef ALLOW_putc
#undef putc
#define putc GPOS_BANNED_API(putc)
#endif

#ifndef ALLOW_qsort
#undef qsort
#define qsort GPOS_BANNED_API(qsort)
#endif

#ifndef ALLOW_putc_unlocked
#undef putc_unlocked
#define putc_unlocked GPOS_BANNED_API(putc_unlocked)
#endif

#ifndef ALLOW_realpath
#undef realpath
#define realpath GPOS_BANNED_API(realpath)
#endif

#ifndef ALLOW_putenv
#undef putenv
#define putenv GPOS_BANNED_API(putenv)
#endif

#ifndef ALLOW_regcomp
#undef regcomp
#define regcomp GPOS_BANNED_API(regcomp)
#endif

#ifndef ALLOW_puts
#undef puts
#define puts GPOS_BANNED_API(puts)
#endif

#ifndef ALLOW_regexec
#undef regexec
#define regexec GPOS_BANNED_API(regexec)
#endif

#ifndef ALLOW_pututline
#undef pututline
#define pututline GPOS_BANNED_API(pututline)
#endif

#ifndef ALLOW_remainder
#undef remainder
#define remainder GPOS_BANNED_API(remainder)
#endif

#ifndef ALLOW_pututxline
#undef pututxline
#define pututxline GPOS_BANNED_API(pututxline)
#endif

#ifndef ALLOW_remainderf
#undef remainderf
#define remainderf GPOS_BANNED_API(remainderf)
#endif

#ifndef ALLOW_putw
#undef putw
#define putw GPOS_BANNED_API(putw)
#endif

#ifndef ALLOW_remainderl
#undef remainderl
#define remainderl GPOS_BANNED_API(remainderl)
#endif

#ifndef ALLOW_putwchar
#undef putwchar
#define putwchar GPOS_BANNED_API(putwchar)
#endif

#ifndef ALLOW_rewind
#undef rewind
#define rewind GPOS_BANNED_API(rewind)
#endif

#ifndef ALLOW_putwchar_unlocked
#undef putwchar_unlocked
#define putwchar_unlocked GPOS_BANNED_API(putwchar_unlocked)
#endif

#ifndef ALLOW_rint
#undef rint
#define rint GPOS_BANNED_API(rint)
#endif

#ifndef ALLOW_qecvt_r
#undef qecvt_r
#define qecvt_r GPOS_BANNED_API(qecvt_r)
#endif

#ifndef ALLOW_rintf
#undef rintf
#define rintf GPOS_BANNED_API(rintf)
#endif

#ifndef ALLOW_raise
#undef raise
#define raise GPOS_BANNED_API(raise)
#endif

#ifndef ALLOW_rintl
#undef rintl
#define rintl GPOS_BANNED_API(rintl)
#endif

#ifndef ALLOW_rand_r
#undef rand_r
#define rand_r GPOS_BANNED_API(rand_r)
#endif

#ifndef ALLOW_rpmatch
#undef rpmatch
#define rpmatch GPOS_BANNED_API(rpmatch)
#endif

#ifndef ALLOW_random_r
#undef random_r
#define random_r GPOS_BANNED_API(random_r)
#endif

#ifndef ALLOW_S_ISBLK
#undef S_ISBLK
#define S_ISBLK GPOS_BANNED_API(S_ISBLK)
#endif

#ifndef ALLOW_rawmemchr
#undef rawmemchr
#define rawmemchr GPOS_BANNED_API(rawmemchr)
#endif

#ifndef ALLOW_S_ISCHR
#undef S_ISCHR
#define S_ISCHR GPOS_BANNED_API(S_ISCHR)
#endif

#ifndef ALLOW_read
#undef read
#define read GPOS_BANNED_API(read)
#endif

#ifndef ALLOW_S_ISDIR
#undef S_ISDIR
#define S_ISDIR GPOS_BANNED_API(S_ISDIR)
#endif

#ifndef ALLOW_readdir64_r
#undef readdir64_r
#define readdir64_r GPOS_BANNED_API(readdir64_r)
#endif

#ifndef ALLOW_S_ISLNK
#undef S_ISLNK
#define S_ISLNK GPOS_BANNED_API(S_ISLNK)
#endif

#ifndef ALLOW_readdir_r
#undef readdir_r
#define readdir_r GPOS_BANNED_API(readdir_r)
#endif

#ifndef ALLOW_S_TYPEISSEM
#undef S_TYPEISSEM
#define S_TYPEISSEM GPOS_BANNED_API(S_TYPEISSEM)
#endif

#ifndef ALLOW_readlink
#undef readlink
#define readlink GPOS_BANNED_API(readlink)
#endif

#ifndef ALLOW_scalb
#undef scalb
#define scalb GPOS_BANNED_API(scalb)
#endif

#ifndef ALLOW_readv
#undef readv
#define readv GPOS_BANNED_API(readv)
#endif

#ifndef ALLOW_scalbf
#undef scalbf
#define scalbf GPOS_BANNED_API(scalbf)
#endif

#ifndef ALLOW_realloc
#undef realloc
#define realloc GPOS_BANNED_API(realloc)
#endif

#ifndef ALLOW_scalbl
#undef scalbl
#define scalbl GPOS_BANNED_API(scalbl)
#endif

#ifndef ALLOW_recv
#undef recv
#define recv GPOS_BANNED_API(recv)
#endif

#ifndef ALLOW_scalbln
#undef scalbln
#define scalbln GPOS_BANNED_API(scalbln)
#endif

#ifndef ALLOW_recvfrom
#undef recvfrom
#define recvfrom GPOS_BANNED_API(recvfrom)
#endif

#ifndef ALLOW_scalblnf
#undef scalblnf
#define scalblnf GPOS_BANNED_API(scalblnf)
#endif

#ifndef ALLOW_regerror
#undef regerror
#define regerror GPOS_BANNED_API(regerror)
#endif

#ifndef ALLOW_scalblnl
#undef scalblnl
#define scalblnl GPOS_BANNED_API(scalblnl)
#endif

#ifndef ALLOW_regfree
#undef regfree
#define regfree GPOS_BANNED_API(regfree)
#endif

#ifndef ALLOW_scalbn
#undef scalbn
#define scalbn GPOS_BANNED_API(scalbn)
#endif

#ifndef ALLOW_register_printf_function
#undef register_printf_function
#define register_printf_function GPOS_BANNED_API(register_printf_function)
#endif

#ifndef ALLOW_scalbnf
#undef scalbnf
#define scalbnf GPOS_BANNED_API(scalbnf)
#endif

#ifndef ALLOW_remove
#undef remove
#define remove GPOS_BANNED_API(remove)
#endif

#ifndef ALLOW_scalbnl
#undef scalbnl
#define scalbnl GPOS_BANNED_API(scalbnl)
#endif

#ifndef ALLOW_rename
#undef rename
#define rename GPOS_BANNED_API(rename)
#endif

#ifndef ALLOW_scanf
#undef scanf
#define scanf GPOS_BANNED_API(scanf)
#endif

#ifndef ALLOW_rewinddir
#undef rewinddir
#define rewinddir GPOS_BANNED_API(rewinddir)
#endif

#ifndef ALLOW_sched_getparam
#undef sched_getparam
#define sched_getparam GPOS_BANNED_API(sched_getparam)
#endif

#ifndef ALLOW_rindex
#undef rindex
#define rindex GPOS_BANNED_API(rindex)
#endif

#ifndef ALLOW_sched_getscheduler
#undef sched_getscheduler
#define sched_getscheduler GPOS_BANNED_API(sched_getscheduler)
#endif

#ifndef ALLOW_rmdir
#undef rmdir
#define rmdir GPOS_BANNED_API(rmdir)
#endif

#ifndef ALLOW_sched_rr_get_interval
#undef sched_rr_get_interval
#define sched_rr_get_interval GPOS_BANNED_API(sched_rr_get_interval)
#endif

#ifndef ALLOW_round
#undef round
#define round GPOS_BANNED_API(round)
#endif

#ifndef ALLOW_sched_setparam
#undef sched_setparam
#define sched_setparam GPOS_BANNED_API(sched_setparam)
#endif

#ifndef ALLOW_roundf
#undef roundf
#define roundf GPOS_BANNED_API(roundf)
#endif

#ifndef ALLOW_sched_setscheduler
#undef sched_setscheduler
#define sched_setscheduler GPOS_BANNED_API(sched_setscheduler)
#endif

#ifndef ALLOW_roundl
#undef roundl
#define roundl GPOS_BANNED_API(roundl)
#endif

#ifndef ALLOW_sched_yield
#undef sched_yield
#define sched_yield GPOS_BANNED_API(sched_yield)
#endif

#ifndef ALLOW_S_ISFIFO
#undef S_ISFIFO
#define S_ISFIFO GPOS_BANNED_API(S_ISFIFO)
#endif

#ifndef ALLOW_seekdir
#undef seekdir
#define seekdir GPOS_BANNED_API(seekdir)
#endif

#ifndef ALLOW_S_ISREG
#undef S_ISREG
#define S_ISREG GPOS_BANNED_API(S_ISREG)
#endif

#ifndef ALLOW_sendto
#undef sendto
#define sendto GPOS_BANNED_API(sendto)
#endif

#ifndef ALLOW_S_ISSOCK
#undef S_ISSOCK
#define S_ISSOCK GPOS_BANNED_API(S_ISSOCK)
#endif

#ifndef ALLOW_setbuf
#undef setbuf
#define setbuf GPOS_BANNED_API(setbuf)
#endif

#ifndef ALLOW_S_TYPEISMQ
#undef S_TYPEISMQ
#define S_TYPEISMQ GPOS_BANNED_API(S_TYPEISMQ)
#endif

#ifndef ALLOW_setcontext
#undef setcontext
#define setcontext GPOS_BANNED_API(setcontext)
#endif

#ifndef ALLOW_S_TYPEISSHM
#undef S_TYPEISSHM
#define S_TYPEISSHM GPOS_BANNED_API(S_TYPEISSHM)
#endif

#ifndef ALLOW_setdomainname
#undef setdomainname
#define setdomainname GPOS_BANNED_API(setdomainname)
#endif

#ifndef ALLOW_scandir
#undef scandir
#define scandir GPOS_BANNED_API(scandir)
#endif

#ifndef ALLOW_setegid
#undef setegid
#define setegid GPOS_BANNED_API(setegid)
#endif

#ifndef ALLOW_scandir64
#undef scandir64
#define scandir64 GPOS_BANNED_API(scandir64)
#endif

#ifndef ALLOW_setenv
#undef setenv
#define setenv GPOS_BANNED_API(setenv)
#endif

#ifndef ALLOW_sched_get_priority_max
#undef sched_get_priority_max
#define sched_get_priority_max GPOS_BANNED_API(sched_get_priority_max)
#endif

#ifndef ALLOW_seteuid
#undef seteuid
#define seteuid GPOS_BANNED_API(seteuid)
#endif

#ifndef ALLOW_sched_get_priority_min
#undef sched_get_priority_min
#define sched_get_priority_min GPOS_BANNED_API(sched_get_priority_min)
#endif

#ifndef ALLOW_sethostent
#undef sethostent
#define sethostent GPOS_BANNED_API(sethostent)
#endif

#ifndef ALLOW_sched_getaffinity
#undef sched_getaffinity
#define sched_getaffinity GPOS_BANNED_API(sched_getaffinity)
#endif

#ifndef ALLOW_sethostid
#undef sethostid
#define sethostid GPOS_BANNED_API(sethostid)
#endif

#ifndef ALLOW_sched_setaffinity
#undef sched_setaffinity
#define sched_setaffinity GPOS_BANNED_API(sched_setaffinity)
#endif

#ifndef ALLOW_sethostname
#undef sethostname
#define sethostname GPOS_BANNED_API(sethostname)
#endif

#ifndef ALLOW_seed48_r
#undef seed48_r
#define seed48_r GPOS_BANNED_API(seed48_r)
#endif

#ifndef ALLOW_setjmp
#undef setjmp
#define setjmp GPOS_BANNED_API(setjmp)
#endif

#ifndef ALLOW_select
#undef select
#define select GPOS_BANNED_API(select)
#endif

#ifndef ALLOW_setlinebuf
#undef setlinebuf
#define setlinebuf GPOS_BANNED_API(setlinebuf)
#endif

#ifndef ALLOW_send
#undef send
#define send GPOS_BANNED_API(send)
#endif

#ifndef ALLOW_setnetgrent
#undef setnetgrent
#define setnetgrent GPOS_BANNED_API(setnetgrent)
#endif

#ifndef ALLOW_setbuffer
#undef setbuffer
#define setbuffer GPOS_BANNED_API(setbuffer)
#endif

#ifndef ALLOW_setpgrp
#undef setpgrp
#define setpgrp GPOS_BANNED_API(setpgrp)
#endif

#ifndef ALLOW_setfsent
#undef setfsent
#define setfsent GPOS_BANNED_API(setfsent)
#endif

#ifndef ALLOW_setprotoent
#undef setprotoent
#define setprotoent GPOS_BANNED_API(setprotoent)
#endif

#ifndef ALLOW_setgid
#undef setgid
#define setgid GPOS_BANNED_API(setgid)
#endif

#ifndef ALLOW_setregid
#undef setregid
#define setregid GPOS_BANNED_API(setregid)
#endif

#ifndef ALLOW_setgrent
#undef setgrent
#define setgrent GPOS_BANNED_API(setgrent)
#endif

#ifndef ALLOW_setreuid
#undef setreuid
#define setreuid GPOS_BANNED_API(setreuid)
#endif

#ifndef ALLOW_setgroups
#undef setgroups
#define setgroups GPOS_BANNED_API(setgroups)
#endif

#ifndef ALLOW_setrlimit
#undef setrlimit
#define setrlimit GPOS_BANNED_API(setrlimit)
#endif

#ifndef ALLOW_setitimer
#undef setitimer
#define setitimer GPOS_BANNED_API(setitimer)
#endif

#ifndef ALLOW_setrlimit64
#undef setrlimit64
#define setrlimit64 GPOS_BANNED_API(setrlimit64)
#endif

#ifndef ALLOW_setkey_r
#undef setkey_r
#define setkey_r GPOS_BANNED_API(setkey_r)
#endif

#ifndef ALLOW_setservent
#undef setservent
#define setservent GPOS_BANNED_API(setservent)
#endif

#ifndef ALLOW_setlocale
#undef setlocale
#define setlocale GPOS_BANNED_API(setlocale)
#endif

#ifndef ALLOW_setsockopt
#undef setsockopt
#define setsockopt GPOS_BANNED_API(setsockopt)
#endif

#ifndef ALLOW_setlogmask
#undef setlogmask
#define setlogmask GPOS_BANNED_API(setlogmask)
#endif

#ifndef ALLOW_setvbuf
#undef setvbuf
#define setvbuf GPOS_BANNED_API(setvbuf)
#endif

#ifndef ALLOW_setmntent
#undef setmntent
#define setmntent GPOS_BANNED_API(setmntent)
#endif

#ifndef ALLOW_sigaction
#undef sigaction
#define sigaction GPOS_BANNED_API(sigaction)
#endif

#ifndef ALLOW_setnetent
#undef setnetent
#define setnetent GPOS_BANNED_API(setnetent)
#endif

#ifndef ALLOW_sigismember
#undef sigismember
#define sigismember GPOS_BANNED_API(sigismember)
#endif

#ifndef ALLOW_setpgid
#undef setpgid
#define setpgid GPOS_BANNED_API(setpgid)
#endif

#ifndef ALLOW_sigmask
#undef sigmask
#define sigmask GPOS_BANNED_API(sigmask)
#endif

#ifndef ALLOW_setpriority
#undef setpriority
#define setpriority GPOS_BANNED_API(setpriority)
#endif

#ifndef ALLOW_significand
#undef significand
#define significand GPOS_BANNED_API(significand)
#endif

#ifndef ALLOW_setpwent
#undef setpwent
#define setpwent GPOS_BANNED_API(setpwent)
#endif

#ifndef ALLOW_significandf
#undef significandf
#define significandf GPOS_BANNED_API(significandf)
#endif

#ifndef ALLOW_setsid
#undef setsid
#define setsid GPOS_BANNED_API(setsid)
#endif

#ifndef ALLOW_significandl
#undef significandl
#define significandl GPOS_BANNED_API(significandl)
#endif

#ifndef ALLOW_setstate_r
#undef setstate_r
#define setstate_r GPOS_BANNED_API(setstate_r)
#endif

#ifndef ALLOW_sigpause
#undef sigpause
#define sigpause GPOS_BANNED_API(sigpause)
#endif

#ifndef ALLOW_settimeofday
#undef settimeofday
#define settimeofday GPOS_BANNED_API(settimeofday)
#endif

#ifndef ALLOW_sigprocmask
#undef sigprocmask
#define sigprocmask GPOS_BANNED_API(sigprocmask)
#endif

#ifndef ALLOW_setuid
#undef setuid
#define setuid GPOS_BANNED_API(setuid)
#endif

#ifndef ALLOW_sigsetmask
#undef sigsetmask
#define sigsetmask GPOS_BANNED_API(sigsetmask)
#endif

#ifndef ALLOW_setutent
#undef setutent
#define setutent GPOS_BANNED_API(setutent)
#endif

#ifndef ALLOW_sigstack
#undef sigstack
#define sigstack GPOS_BANNED_API(sigstack)
#endif

#ifndef ALLOW_setutxent
#undef setutxent
#define setutxent GPOS_BANNED_API(setutxent)
#endif

#ifndef ALLOW_sigsuspend
#undef sigsuspend
#define sigsuspend GPOS_BANNED_API(sigsuspend)
#endif

#ifndef ALLOW_shutdown
#undef shutdown
#define shutdown GPOS_BANNED_API(shutdown)
#endif

#ifndef ALLOW_sigvec
#undef sigvec
#define sigvec GPOS_BANNED_API(sigvec)
#endif

#ifndef ALLOW_sigaddset
#undef sigaddset
#define sigaddset GPOS_BANNED_API(sigaddset)
#endif

#ifndef ALLOW_sincos
#undef sincos
#define sincos GPOS_BANNED_API(sincos)
#endif

#ifndef ALLOW_sigaltstack
#undef sigaltstack
#define sigaltstack GPOS_BANNED_API(sigaltstack)
#endif

#ifndef ALLOW_sincosf
#undef sincosf
#define sincosf GPOS_BANNED_API(sincosf)
#endif

#ifndef ALLOW_sigblock
#undef sigblock
#define sigblock GPOS_BANNED_API(sigblock)
#endif

#ifndef ALLOW_sincosl
#undef sincosl
#define sincosl GPOS_BANNED_API(sincosl)
#endif

#ifndef ALLOW_sigdelset
#undef sigdelset
#define sigdelset GPOS_BANNED_API(sigdelset)
#endif

#ifndef ALLOW_sleep
#undef sleep
#define sleep GPOS_BANNED_API(sleep)
#endif

#ifndef ALLOW_sigemptyset
#undef sigemptyset
#define sigemptyset GPOS_BANNED_API(sigemptyset)
#endif

#ifndef ALLOW_socket
#undef socket
#define socket GPOS_BANNED_API(socket)
#endif

#ifndef ALLOW_sigfillset
#undef sigfillset
#define sigfillset GPOS_BANNED_API(sigfillset)
#endif

#ifndef ALLOW_socketpair
#undef socketpair
#define socketpair GPOS_BANNED_API(socketpair)
#endif

#ifndef ALLOW_siginterrupt
#undef siginterrupt
#define siginterrupt GPOS_BANNED_API(siginterrupt)
#endif

#ifndef ALLOW_srand48_r
#undef srand48_r
#define srand48_r GPOS_BANNED_API(srand48_r)
#endif

#ifndef ALLOW_siglongjmp
#undef siglongjmp
#define siglongjmp GPOS_BANNED_API(siglongjmp)
#endif

#ifndef ALLOW_srandom_r
#undef srandom_r
#define srandom_r GPOS_BANNED_API(srandom_r)
#endif

#ifndef ALLOW_signal
#undef signal
#define signal GPOS_BANNED_API(signal)
#endif

#ifndef ALLOW_ssignal
#undef ssignal
#define ssignal GPOS_BANNED_API(ssignal)
#endif

#ifndef ALLOW_signbit
#undef signbit
#define signbit GPOS_BANNED_API(signbit)
#endif

#ifndef ALLOW_stpcpy
#undef stpcpy
#define stpcpy GPOS_BANNED_API(stpcpy)
#endif

#ifndef ALLOW_sigpending
#undef sigpending
#define sigpending GPOS_BANNED_API(sigpending)
#endif

#ifndef ALLOW_stpncpy
#undef stpncpy
#define stpncpy GPOS_BANNED_API(stpncpy)
#endif

#ifndef ALLOW_sigsetjmp
#undef sigsetjmp
#define sigsetjmp GPOS_BANNED_API(sigsetjmp)
#endif

#ifndef ALLOW_strcasecmp
#undef strcasecmp
#define strcasecmp GPOS_BANNED_API(strcasecmp)
#endif

#ifndef ALLOW_sin
#undef sin
#define sin GPOS_BANNED_API(sin)
#endif

#ifndef ALLOW_strcat
#undef strcat
#define strcat GPOS_BANNED_API(strcat)
#endif

#ifndef ALLOW_sinf
#undef sinf
#define sinf GPOS_BANNED_API(sinf)
#endif

#ifndef ALLOW_strchrnul
#undef strchrnul
#define strchrnul GPOS_BANNED_API(strchrnul)
#endif

#ifndef ALLOW_sinh
#undef sinh
#define sinh GPOS_BANNED_API(sinh)
#endif

#ifndef ALLOW_strcmp
#undef strcmp
#define strcmp GPOS_BANNED_API(strcmp)
#endif

#ifndef ALLOW_sinhf
#undef sinhf
#define sinhf GPOS_BANNED_API(sinhf)
#endif

#ifndef ALLOW_strcoll
#undef strcoll
#define strcoll GPOS_BANNED_API(strcoll)
#endif

#ifndef ALLOW_sinhl
#undef sinhl
#define sinhl GPOS_BANNED_API(sinhl)
#endif

#ifndef ALLOW_strcpy
#undef strcpy
#define strcpy GPOS_BANNED_API(strcpy)
#endif

#ifndef ALLOW_sinl
#undef sinl
#define sinl GPOS_BANNED_API(sinl)
#endif

#ifndef ALLOW_strcspn
#undef strcspn
#define strcspn GPOS_BANNED_API(strcspn)
#endif

#ifndef ALLOW_snprintf
#undef snprintf
#define snprintf GPOS_BANNED_API(snprintf)
#endif

#ifndef ALLOW_strdupa
#undef strdupa
#define strdupa GPOS_BANNED_API(strdupa)
#endif

#ifndef ALLOW_sprintf
#undef sprintf
#define sprintf GPOS_BANNED_API(sprintf)
#endif

#ifndef ALLOW_strfmon
#undef strfmon
#define strfmon GPOS_BANNED_API(strfmon)
#endif

#ifndef ALLOW_sqrt
#undef sqrt
#define sqrt GPOS_BANNED_API(sqrt)
#endif

#ifndef ALLOW_strncasecmp
#undef strncasecmp
#define strncasecmp GPOS_BANNED_API(strncasecmp)
#endif

#ifndef ALLOW_sqrtf
#undef sqrtf
#define sqrtf GPOS_BANNED_API(sqrtf)
#endif

#ifndef ALLOW_strncat
#undef strncat
#define strncat GPOS_BANNED_API(strncat)
#endif

#ifndef ALLOW_sqrtl
#undef sqrtl
#define sqrtl GPOS_BANNED_API(sqrtl)
#endif

#ifndef ALLOW_strncmp
#undef strncmp
#define strncmp GPOS_BANNED_API(strncmp)
#endif

#ifndef ALLOW_srand
#undef srand
#define srand GPOS_BANNED_API(srand)
#endif

#ifndef ALLOW_strncpy
#undef strncpy
#define strncpy GPOS_BANNED_API(strncpy)
#endif

#ifndef ALLOW_sscanf
#undef sscanf
#define sscanf GPOS_BANNED_API(sscanf)
#endif

#ifndef ALLOW_strndupa
#undef strndupa
#define strndupa GPOS_BANNED_API(strndupa)
#endif

#ifndef ALLOW_stat
#undef stat
#define stat GPOS_BANNED_API(stat)
#endif

#ifndef ALLOW_strsep
#undef strsep
#define strsep GPOS_BANNED_API(strsep)
#endif

#ifndef ALLOW_stat64
#undef stat64
#define stat64 GPOS_BANNED_API(stat64)
#endif

#ifndef ALLOW_strsignal
#undef strsignal
#define strsignal GPOS_BANNED_API(strsignal)
#endif

#ifndef ALLOW_stime
#undef stime
#define stime GPOS_BANNED_API(stime)
#endif

#ifndef ALLOW_strtoimax
#undef strtoimax
#define strtoimax GPOS_BANNED_API(strtoimax)
#endif

#ifndef ALLOW_strcasestr
#undef strcasestr
#define strcasestr GPOS_BANNED_API(strcasestr)
#endif

#ifndef ALLOW_strtoq
#undef strtoq
#define strtoq GPOS_BANNED_API(strtoq)
#endif

#ifndef ALLOW_strchr
#undef strchr
#define strchr GPOS_BANNED_API(strchr)
#endif

#ifndef ALLOW_strtoul
#undef strtoul
#define strtoul GPOS_BANNED_API(strtoul)
#endif

#ifndef ALLOW_strdup
#undef strdup
#define strdup GPOS_BANNED_API(strdup)
#endif

#ifndef ALLOW_strtoull
#undef strtoull
#define strtoull GPOS_BANNED_API(strtoull)
#endif

#ifndef ALLOW_strerror_r
#undef strerror_r
#define strerror_r GPOS_BANNED_API(strerror_r)
#endif

#ifndef ALLOW_strtoumax
#undef strtoumax
#define strtoumax GPOS_BANNED_API(strtoumax)
#endif

#ifndef ALLOW_strfry
#undef strfry
#define strfry GPOS_BANNED_API(strfry)
#endif

#ifndef ALLOW_strverscmp
#undef strverscmp
#define strverscmp GPOS_BANNED_API(strverscmp)
#endif

#ifndef ALLOW_strftime
#undef strftime
#define strftime GPOS_BANNED_API(strftime)
#endif

#ifndef ALLOW_success
#undef success
#define success GPOS_BANNED_API(success)
#endif

#ifndef ALLOW_strlen
#undef strlen
#define strlen GPOS_BANNED_API(strlen)
#endif

#ifndef ALLOW_swprintf
#undef swprintf
#define swprintf GPOS_BANNED_API(swprintf)
#endif

#ifndef ALLOW_strndup
#undef strndup
#define strndup GPOS_BANNED_API(strndup)
#endif

#ifndef ALLOW_swscanf
#undef swscanf
#define swscanf GPOS_BANNED_API(swscanf)
#endif

#ifndef ALLOW_strnlen
#undef strnlen
#define strnlen GPOS_BANNED_API(strnlen)
#endif

#ifndef ALLOW_symlink
#undef symlink
#define symlink GPOS_BANNED_API(symlink)
#endif

#ifndef ALLOW_strpbrk
#undef strpbrk
#define strpbrk GPOS_BANNED_API(strpbrk)
#endif

#ifndef ALLOW_sync
#undef sync
#define sync GPOS_BANNED_API(sync)
#endif

#ifndef ALLOW_strptime
#undef strptime
#define strptime GPOS_BANNED_API(strptime)
#endif

#ifndef ALLOW_syscall
#undef syscall
#define syscall GPOS_BANNED_API(syscall)
#endif

#ifndef ALLOW_strrchr
#undef strrchr
#define strrchr GPOS_BANNED_API(strrchr)
#endif

#ifndef ALLOW_sysconf
#undef sysconf
#define sysconf GPOS_BANNED_API(sysconf)
#endif

#ifndef ALLOW_strspn
#undef strspn
#define strspn GPOS_BANNED_API(strspn)
#endif

#ifndef ALLOW_syslog
#undef syslog
#define syslog GPOS_BANNED_API(syslog)
#endif

#ifndef ALLOW_strstr
#undef strstr
#define strstr GPOS_BANNED_API(strstr)
#endif

#ifndef ALLOW_system
#undef system
#define system GPOS_BANNED_API(system)
#endif

#ifndef ALLOW_strtod
#undef strtod
#define strtod GPOS_BANNED_API(strtod)
#endif

#ifndef ALLOW_tan
#undef tan
#define tan GPOS_BANNED_API(tan)
#endif

#ifndef ALLOW_strtof
#undef strtof
#define strtof GPOS_BANNED_API(strtof)
#endif

#ifndef ALLOW_tanf
#undef tanf
#define tanf GPOS_BANNED_API(tanf)
#endif

#ifndef ALLOW_strtok_r
#undef strtok_r
#define strtok_r GPOS_BANNED_API(strtok_r)
#endif

#ifndef ALLOW_tanh
#undef tanh
#define tanh GPOS_BANNED_API(tanh)
#endif

#ifndef ALLOW_strtol
#undef strtol
#define strtol GPOS_BANNED_API(strtol)
#endif

#ifndef ALLOW_tanhf
#undef tanhf
#define tanhf GPOS_BANNED_API(tanhf)
#endif

#ifndef ALLOW_strtold
#undef strtold
#define strtold GPOS_BANNED_API(strtold)
#endif

#ifndef ALLOW_tanhl
#undef tanhl
#define tanhl GPOS_BANNED_API(tanhl)
#endif

#ifndef ALLOW_strtoll
#undef strtoll
#define strtoll GPOS_BANNED_API(strtoll)
#endif

#ifndef ALLOW_tanl
#undef tanl
#define tanl GPOS_BANNED_API(tanl)
#endif

#ifndef ALLOW_strtouq
#undef strtouq
#define strtouq GPOS_BANNED_API(strtouq)
#endif

#ifndef ALLOW_tcdrain
#undef tcdrain
#define tcdrain GPOS_BANNED_API(tcdrain)
#endif

#ifndef ALLOW_strxfrm
#undef strxfrm
#define strxfrm GPOS_BANNED_API(strxfrm)
#endif

#ifndef ALLOW_tcflow
#undef tcflow
#define tcflow GPOS_BANNED_API(tcflow)
#endif

#ifndef ALLOW_stty
#undef stty
#define stty GPOS_BANNED_API(stty)
#endif

#ifndef ALLOW_tcflush
#undef tcflush
#define tcflush GPOS_BANNED_API(tcflush)
#endif

#ifndef ALLOW_SUN_LEN
#undef SUN_LEN
#define SUN_LEN GPOS_BANNED_API(SUN_LEN)
#endif

#ifndef ALLOW_tcgetsid
#undef tcgetsid
#define tcgetsid GPOS_BANNED_API(tcgetsid)
#endif

#ifndef ALLOW_swapcontext
#undef swapcontext
#define swapcontext GPOS_BANNED_API(swapcontext)
#endif

#ifndef ALLOW_tdelete
#undef tdelete
#define tdelete GPOS_BANNED_API(tdelete)
#endif

#ifndef ALLOW_sysctl
#undef sysctl
#define sysctl GPOS_BANNED_API(sysctl)
#endif

#ifndef ALLOW_textdomain
#undef textdomain
#define textdomain GPOS_BANNED_API(textdomain)
#endif

#ifndef ALLOW_sysv_signal
#undef sysv_signal
#define sysv_signal GPOS_BANNED_API(sysv_signal)
#endif

#ifndef ALLOW_tfind
#undef tfind
#define tfind GPOS_BANNED_API(tfind)
#endif

#ifndef ALLOW_tcgetattr
#undef tcgetattr
#define tcgetattr GPOS_BANNED_API(tcgetattr)
#endif

#ifndef ALLOW_tgamma
#undef tgamma
#define tgamma GPOS_BANNED_API(tgamma)
#endif

#ifndef ALLOW_tcgetpgrp
#undef tcgetpgrp
#define tcgetpgrp GPOS_BANNED_API(tcgetpgrp)
#endif

#ifndef ALLOW_tgammaf
#undef tgammaf
#define tgammaf GPOS_BANNED_API(tgammaf)
#endif

#ifndef ALLOW_tcsendbreak
#undef tcsendbreak
#define tcsendbreak GPOS_BANNED_API(tcsendbreak)
#endif

#ifndef ALLOW_tgammal
#undef tgammal
#define tgammal GPOS_BANNED_API(tgammal)
#endif

#ifndef ALLOW_tcsetattr
#undef tcsetattr
#define tcsetattr GPOS_BANNED_API(tcsetattr)
#endif

#ifndef ALLOW_time
#undef time
#define time GPOS_BANNED_API(time)
#endif

#ifndef ALLOW_tcsetpgrp
#undef tcsetpgrp
#define tcsetpgrp GPOS_BANNED_API(tcsetpgrp)
#endif

#ifndef ALLOW_timegm
#undef timegm
#define timegm GPOS_BANNED_API(timegm)
#endif

#ifndef ALLOW_tdestroy
#undef tdestroy
#define tdestroy GPOS_BANNED_API(tdestroy)
#endif

#ifndef ALLOW_tmpfile
#undef tmpfile
#define tmpfile GPOS_BANNED_API(tmpfile)
#endif

#ifndef ALLOW_TEMP_FAILURE_RETRY
#undef TEMP_FAILURE_RETRY
#define TEMP_FAILURE_RETRY GPOS_BANNED_API(TEMP_FAILURE_RETRY)
#endif

#ifndef ALLOW_tmpfile64
#undef tmpfile64
#define tmpfile64 GPOS_BANNED_API(tmpfile64)
#endif

#ifndef ALLOW_tempnam
#undef tempnam
#define tempnam GPOS_BANNED_API(tempnam)
#endif

#ifndef ALLOW_toupper
#undef toupper
#define toupper GPOS_BANNED_API(toupper)
#endif

#ifndef ALLOW_timelocal
#undef timelocal
#define timelocal GPOS_BANNED_API(timelocal)
#endif

#ifndef ALLOW_towctrans
#undef towctrans
#define towctrans GPOS_BANNED_API(towctrans)
#endif

#ifndef ALLOW_times
#undef times
#define times GPOS_BANNED_API(times)
#endif

#ifndef ALLOW_towlower
#undef towlower
#define towlower GPOS_BANNED_API(towlower)
#endif

#ifndef ALLOW_tmpnam_r
#undef tmpnam_r
#define tmpnam_r GPOS_BANNED_API(tmpnam_r)
#endif

#ifndef ALLOW_tryagain
#undef tryagain
#define tryagain GPOS_BANNED_API(tryagain)
#endif

#ifndef ALLOW_toascii
#undef toascii
#define toascii GPOS_BANNED_API(toascii)
#endif

#ifndef ALLOW_ttyname_r
#undef ttyname_r
#define ttyname_r GPOS_BANNED_API(ttyname_r)
#endif

#ifndef ALLOW_tolower
#undef tolower
#define tolower GPOS_BANNED_API(tolower)
#endif

#ifndef ALLOW_twalk
#undef twalk
#define twalk GPOS_BANNED_API(twalk)
#endif

#ifndef ALLOW_towupper
#undef towupper
#define towupper GPOS_BANNED_API(towupper)
#endif

#ifndef ALLOW_umask
#undef umask
#define umask GPOS_BANNED_API(umask)
#endif

#ifndef ALLOW_trunc
#undef trunc
#define trunc GPOS_BANNED_API(trunc)
#endif

#ifndef ALLOW_ungetwc
#undef ungetwc
#define ungetwc GPOS_BANNED_API(ungetwc)
#endif

#ifndef ALLOW_truncate
#undef truncate
#define truncate GPOS_BANNED_API(truncate)
#endif

#ifndef ALLOW_unlink
#undef unlink
#define unlink GPOS_BANNED_API(unlink)
#endif

#ifndef ALLOW_truncate64
#undef truncate64
#define truncate64 GPOS_BANNED_API(truncate64)
#endif

#ifndef ALLOW_updwtmp
#undef updwtmp
#define updwtmp GPOS_BANNED_API(updwtmp)
#endif

#ifndef ALLOW_truncf
#undef truncf
#define truncf GPOS_BANNED_API(truncf)
#endif

#ifndef ALLOW_utimes
#undef utimes
#define utimes GPOS_BANNED_API(utimes)
#endif

#ifndef ALLOW_truncl
#undef truncl
#define truncl GPOS_BANNED_API(truncl)
#endif

#ifndef ALLOW_utmpname
#undef utmpname
#define utmpname GPOS_BANNED_API(utmpname)
#endif

#ifndef ALLOW_tsearch
#undef tsearch
#define tsearch GPOS_BANNED_API(tsearch)
#endif

#ifndef ALLOW_utmpxname
#undef utmpxname
#define utmpxname GPOS_BANNED_API(utmpxname)
#endif

#ifndef ALLOW_tzset
#undef tzset
#define tzset GPOS_BANNED_API(tzset)
#endif

#ifndef ALLOW_va_alist
#undef va_alist
#define va_alist GPOS_BANNED_API(va_alist)
#endif

#ifndef ALLOW_ulimit
#undef ulimit
#define ulimit GPOS_BANNED_API(ulimit)
#endif

#ifndef ALLOW_va_dcl
#undef va_dcl
#define va_dcl GPOS_BANNED_API(va_dcl)
#endif

#ifndef ALLOW_umount
#undef umount
#define umount GPOS_BANNED_API(umount)
#endif

#ifndef ALLOW_va_end
#undef va_end
#define va_end GPOS_BANNED_API(va_end)
#endif

#ifndef ALLOW_umount2
#undef umount2
#define umount2 GPOS_BANNED_API(umount2)
#endif

#ifndef ALLOW_valloc
#undef valloc
#define valloc GPOS_BANNED_API(valloc)
#endif

#ifndef ALLOW_uname
#undef uname
#define uname GPOS_BANNED_API(uname)
#endif

#ifndef ALLOW_vasprintf
#undef vasprintf
#define vasprintf GPOS_BANNED_API(vasprintf)
#endif

#ifndef ALLOW_unavail
#undef unavail
#define unavail GPOS_BANNED_API(unavail)
#endif

#ifndef ALLOW_verr
#undef verr
#define verr GPOS_BANNED_API(verr)
#endif

#ifndef ALLOW_ungetc
#undef ungetc
#define ungetc GPOS_BANNED_API(ungetc)
#endif

#ifndef ALLOW_verrx
#undef verrx
#define verrx GPOS_BANNED_API(verrx)
#endif

#ifndef ALLOW_unlockpt
#undef unlockpt
#define unlockpt GPOS_BANNED_API(unlockpt)
#endif

#ifndef ALLOW_vfprintf
#undef vfprintf
#define vfprintf GPOS_BANNED_API(vfprintf)
#endif

#ifndef ALLOW_unsetenv
#undef unsetenv
#define unsetenv GPOS_BANNED_API(unsetenv)
#endif

#ifndef ALLOW_vfscanf
#undef vfscanf
#define vfscanf GPOS_BANNED_API(vfscanf)
#endif

#ifndef ALLOW_utime
#undef utime
#define utime GPOS_BANNED_API(utime)
#endif

#ifndef ALLOW_vlimit
#undef vlimit
#define vlimit GPOS_BANNED_API(vlimit)
#endif

#ifndef ALLOW_va_arg
#undef va_arg
#define va_arg GPOS_BANNED_API(va_arg)
#endif

#ifndef ALLOW_vprintf
#undef vprintf
#define vprintf GPOS_BANNED_API(vprintf)
#endif

#ifndef ALLOW_va_start
#undef va_start
#define va_start GPOS_BANNED_API(va_start)
#endif

#ifndef ALLOW_vscanf
#undef vscanf
#define vscanf GPOS_BANNED_API(vscanf)
#endif

#ifndef ALLOW_va_start
#undef va_start
#define va_start GPOS_BANNED_API(va_start)
#endif

#ifndef ALLOW_vswprintf
#undef vswprintf
#define vswprintf GPOS_BANNED_API(vswprintf)
#endif

#ifndef ALLOW_versionsort
#undef versionsort
#define versionsort GPOS_BANNED_API(versionsort)
#endif

#ifndef ALLOW_vswscanf
#undef vswscanf
#define vswscanf GPOS_BANNED_API(vswscanf)
#endif

#ifndef ALLOW_versionsort64
#undef versionsort64
#define versionsort64 GPOS_BANNED_API(versionsort64)
#endif

#ifndef ALLOW_vsyslog
#undef vsyslog
#define vsyslog GPOS_BANNED_API(vsyslog)
#endif

#ifndef ALLOW_vfork
#undef vfork
#define vfork GPOS_BANNED_API(vfork)
#endif

#ifndef ALLOW_wait
#undef wait
#define wait GPOS_BANNED_API(wait)
#endif

#ifndef ALLOW_vfwprintf
#undef vfwprintf
#define vfwprintf GPOS_BANNED_API(vfwprintf)
#endif

#ifndef ALLOW_wait4
#undef wait4
#define wait4 GPOS_BANNED_API(wait4)
#endif

#ifndef ALLOW_vfwscanf
#undef vfwscanf
#define vfwscanf GPOS_BANNED_API(vfwscanf)
#endif

#ifndef ALLOW_wcscasecmp
#undef wcscasecmp
#define wcscasecmp GPOS_BANNED_API(wcscasecmp)
#endif

#ifndef ALLOW_vsnprintf
#undef vsnprintf
#define vsnprintf GPOS_BANNED_API(vsnprintf)
#endif

#ifndef ALLOW_wcscat
#undef wcscat
#define wcscat GPOS_BANNED_API(wcscat)
#endif

#ifndef ALLOW_vsprintf
#undef vsprintf
#define vsprintf GPOS_BANNED_API(vsprintf)
#endif

#ifndef ALLOW_wcschrnul
#undef wcschrnul
#define wcschrnul GPOS_BANNED_API(wcschrnul)
#endif

#ifndef ALLOW_vsscanf
#undef vsscanf
#define vsscanf GPOS_BANNED_API(vsscanf)
#endif

#ifndef ALLOW_wcscmp
#undef wcscmp
#define wcscmp GPOS_BANNED_API(wcscmp)
#endif

#ifndef ALLOW_vtimes
#undef vtimes
#define vtimes GPOS_BANNED_API(vtimes)
#endif

#ifndef ALLOW_wcscoll
#undef wcscoll
#define wcscoll GPOS_BANNED_API(wcscoll)
#endif

#ifndef ALLOW_vwarn
#undef vwarn
#define vwarn GPOS_BANNED_API(vwarn)
#endif

#ifndef ALLOW_wcscpy
#undef wcscpy
#define wcscpy GPOS_BANNED_API(wcscpy)
#endif

#ifndef ALLOW_vwarnx
#undef vwarnx
#define vwarnx GPOS_BANNED_API(vwarnx)
#endif

#ifndef ALLOW_wcscspn
#undef wcscspn
#define wcscspn GPOS_BANNED_API(wcscspn)
#endif

#ifndef ALLOW_vwprintf
#undef vwprintf
#define vwprintf GPOS_BANNED_API(vwprintf)
#endif

#ifndef ALLOW_wcsncasecmp
#undef wcsncasecmp
#define wcsncasecmp GPOS_BANNED_API(wcsncasecmp)
#endif

#ifndef ALLOW_vwscanf
#undef vwscanf
#define vwscanf GPOS_BANNED_API(vwscanf)
#endif

#ifndef ALLOW_wcsncat
#undef wcsncat
#define wcsncat GPOS_BANNED_API(wcsncat)
#endif

#ifndef ALLOW_wait3
#undef wait3
#define wait3 GPOS_BANNED_API(wait3)
#endif

#ifndef ALLOW_wcsncmp
#undef wcsncmp
#define wcsncmp GPOS_BANNED_API(wcsncmp)
#endif

#ifndef ALLOW_waitpid
#undef waitpid
#define waitpid GPOS_BANNED_API(waitpid)
#endif

#ifndef ALLOW_wcsncpy
#undef wcsncpy
#define wcsncpy GPOS_BANNED_API(wcsncpy)
#endif

#ifndef ALLOW_warn
#undef warn
#define warn GPOS_BANNED_API(warn)
#endif

#ifndef ALLOW_wcstoimax
#undef wcstoimax
#define wcstoimax GPOS_BANNED_API(wcstoimax)
#endif

#ifndef ALLOW_warnx
#undef warnx
#define warnx GPOS_BANNED_API(warnx)
#endif

#ifndef ALLOW_wcstok
#undef wcstok
#define wcstok GPOS_BANNED_API(wcstok)
#endif

#ifndef ALLOW_WCOREDUMP
#undef WCOREDUMP
#define WCOREDUMP GPOS_BANNED_API(WCOREDUMP)
#endif

#ifndef ALLOW_wcstoq
#undef wcstoq
#define wcstoq GPOS_BANNED_API(wcstoq)
#endif

#ifndef ALLOW_wcpcpy
#undef wcpcpy
#define wcpcpy GPOS_BANNED_API(wcpcpy)
#endif

#ifndef ALLOW_wcstoul
#undef wcstoul
#define wcstoul GPOS_BANNED_API(wcstoul)
#endif

#ifndef ALLOW_wcpncpy
#undef wcpncpy
#define wcpncpy GPOS_BANNED_API(wcpncpy)
#endif

#ifndef ALLOW_wcstoull
#undef wcstoull
#define wcstoull GPOS_BANNED_API(wcstoull)
#endif

#ifndef ALLOW_wcrtomb
#undef wcrtomb
#define wcrtomb GPOS_BANNED_API(wcrtomb)
#endif

#ifndef ALLOW_wcstoumax
#undef wcstoumax
#define wcstoumax GPOS_BANNED_API(wcstoumax)
#endif

#ifndef ALLOW_wcschr
#undef wcschr
#define wcschr GPOS_BANNED_API(wcschr)
#endif

#ifndef ALLOW_wctob
#undef wctob
#define wctob GPOS_BANNED_API(wctob)
#endif

#ifndef ALLOW_wcsdup
#undef wcsdup
#define wcsdup GPOS_BANNED_API(wcsdup)
#endif

#ifndef ALLOW_WEXITSTATUS
#undef WEXITSTATUS
#define WEXITSTATUS GPOS_BANNED_API(WEXITSTATUS)
#endif

#ifndef ALLOW_wcsftime
#undef wcsftime
#define wcsftime GPOS_BANNED_API(wcsftime)
#endif

#ifndef ALLOW_WIFEXITED
#undef WIFEXITED
#define WIFEXITED GPOS_BANNED_API(WIFEXITED)
#endif

#ifndef ALLOW_wcslen
#undef wcslen
#define wcslen GPOS_BANNED_API(wcslen)
#endif

#ifndef ALLOW_WIFSIGNALED
#undef WIFSIGNALED
#define WIFSIGNALED GPOS_BANNED_API(WIFSIGNALED)
#endif

#ifndef ALLOW_wcsnlen
#undef wcsnlen
#define wcsnlen GPOS_BANNED_API(wcsnlen)
#endif

#ifndef ALLOW_WIFSTOPPED
#undef WIFSTOPPED
#define WIFSTOPPED GPOS_BANNED_API(WIFSTOPPED)
#endif

#ifndef ALLOW_wcsnrtombs
#undef wcsnrtombs
#define wcsnrtombs GPOS_BANNED_API(wcsnrtombs)
#endif

#ifndef ALLOW_wmemchr
#undef wmemchr
#define wmemchr GPOS_BANNED_API(wmemchr)
#endif

#ifndef ALLOW_wcspbrk
#undef wcspbrk
#define wcspbrk GPOS_BANNED_API(wcspbrk)
#endif

#ifndef ALLOW_wmemmove
#undef wmemmove
#define wmemmove GPOS_BANNED_API(wmemmove)
#endif

#ifndef ALLOW_wcsrchr
#undef wcsrchr
#define wcsrchr GPOS_BANNED_API(wcsrchr)
#endif

#ifndef ALLOW_wordexp
#undef wordexp
#define wordexp GPOS_BANNED_API(wordexp)
#endif

#ifndef ALLOW_wcsrtombs
#undef wcsrtombs
#define wcsrtombs GPOS_BANNED_API(wcsrtombs)
#endif

#ifndef ALLOW_write
#undef write
#define write GPOS_BANNED_API(write)
#endif

#ifndef ALLOW_wcsspn
#undef wcsspn
#define wcsspn GPOS_BANNED_API(wcsspn)
#endif

#ifndef ALLOW_writev
#undef writev
#define writev GPOS_BANNED_API(writev)
#endif

#ifndef ALLOW_wcsstr
#undef wcsstr
#define wcsstr GPOS_BANNED_API(wcsstr)
#endif

#ifndef ALLOW_y0
#undef y0
#define y0 GPOS_BANNED_API(y0)
#endif

#ifndef ALLOW_wcstod
#undef wcstod
#define wcstod GPOS_BANNED_API(wcstod)
#endif

#ifndef ALLOW_y0f
#undef y0f
#define y0f GPOS_BANNED_API(y0f)
#endif

#ifndef ALLOW_wcstof
#undef wcstof
#define wcstof GPOS_BANNED_API(wcstof)
#endif

#ifndef ALLOW_y0l
#undef y0l
#define y0l GPOS_BANNED_API(y0l)
#endif

#ifndef ALLOW_wcstol
#undef wcstol
#define wcstol GPOS_BANNED_API(wcstol)
#endif

#ifndef ALLOW_yn
#undef yn
#define yn GPOS_BANNED_API(yn)
#endif

#ifndef ALLOW_wcstold
#undef wcstold
#define wcstold GPOS_BANNED_API(wcstold)
#endif

#ifndef ALLOW_ynf
#undef ynf
#define ynf GPOS_BANNED_API(ynf)
#endif

#ifndef ALLOW_wcstoll
#undef wcstoll
#define wcstoll GPOS_BANNED_API(wcstoll)
#endif

#ifndef ALLOW_ynl
#undef ynl
#define ynl GPOS_BANNED_API(ynl)
#endif

#ifndef ALLOW_wcstombs
#undef wcstombs
#define wcstombs GPOS_BANNED_API(wcstombs)
#endif

#ifndef ALLOW_wcstouq
#undef wcstouq
#define wcstouq GPOS_BANNED_API(wcstouq)
#endif

#ifndef ALLOW_wcswcs
#undef wcswcs
#define wcswcs GPOS_BANNED_API(wcswcs)
#endif

#ifndef ALLOW_wcsxfrm
#undef wcsxfrm
#define wcsxfrm GPOS_BANNED_API(wcsxfrm)
#endif

#ifndef ALLOW_wctomb
#undef wctomb
#define wctomb GPOS_BANNED_API(wctomb)
#endif

#ifndef ALLOW_wctrans
#undef wctrans
#define wctrans GPOS_BANNED_API(wctrans)
#endif

#ifndef ALLOW_wctype
#undef wctype
#define wctype GPOS_BANNED_API(wctype)
#endif

#ifndef ALLOW_wmemcmp
#undef wmemcmp
#define wmemcmp GPOS_BANNED_API(wmemcmp)
#endif

#ifndef ALLOW_wmemcpy
#undef wmemcpy
#define wmemcpy GPOS_BANNED_API(wmemcpy)
#endif

#ifndef ALLOW_wmempcpy
#undef wmempcpy
#define wmempcpy GPOS_BANNED_API(wmempcpy)
#endif

#ifndef ALLOW_wmemset
#undef wmemset
#define wmemset GPOS_BANNED_API(wmemset)
#endif

#ifndef ALLOW_wordfree
#undef wordfree
#define wordfree GPOS_BANNED_API(wordfree)
#endif

#ifndef ALLOW_wprintf
#undef wprintf
#define wprintf GPOS_BANNED_API(wprintf)
#endif

#ifndef ALLOW_wscanf
#undef wscanf
#define wscanf GPOS_BANNED_API(wscanf)
#endif

#ifndef ALLOW_WSTOPSIG
#undef WSTOPSIG
#define WSTOPSIG GPOS_BANNED_API(WSTOPSIG)
#endif

#ifndef ALLOW_WTERMSIG
#undef WTERMSIG
#define WTERMSIG GPOS_BANNED_API(WTERMSIG)
#endif

#ifndef ALLOW_y1
#undef y1
#define y1 GPOS_BANNED_API(y1)
#endif

#ifndef ALLOW_y1f
#undef y1f
#define y1f GPOS_BANNED_API(y1f)
#endif

#ifndef ALLOW_y1l
#undef y1l
#define y1l GPOS_BANNED_API(y1l)
#endif

#endif //#ifndef BANNED_POSIX_REENTRANT_H

// EOF

