/*
 * Generated Mocking Source
 */
#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"


#include "postgres.h"

#include <fcntl.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <ctype.h>
#ifdef HAVE_SYSLOG
#include <syslog.h>
#endif
#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif

#ifdef HAVE_EXECINFO_H
#include <execinfo.h>
#endif

#include "access/transam.h"
#include "access/xact.h"
#include "libpq/libpq.h"
#include "libpq/pqformat.h"
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "postmaster/syslogger.h"
#include "storage/ipc.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"

#include "cdb/cdbvars.h"  
#include "cdb/cdbtm.h"
#include "utils/ps_status.h"    
#include "cdb/cdbselect.h"
#include "pgtime.h"

#include "miscadmin.h"


#define _DARWIN_C_SOURCE 1
#include <dlfcn.h>


#undef _
#define _(x) err_gettext(x)



ErrorContextCallback *error_context_stack = NULL;

sigjmp_buf *PG_exception_stack = NULL;

extern bool redirection_done;


emit_log_hook_type emit_log_hook = NULL;


int			Log_error_verbosity = PGERROR_VERBOSE;
char	   *Log_line_prefix = NULL; 
int			Log_destination = LOG_DESTINATION_STDERR;
char	   *Log_destination_string = NULL;
bool		syslog_sequence_numbers = true;
bool		syslog_split_messages = true;

#ifdef HAVE_SYSLOG


#ifndef PG_SYSLOG_LIMIT
#define PG_SYSLOG_LIMIT 900
#endif

static bool openlog_done = false;
static char *syslog_ident = NULL;
static int	syslog_facility = LOG_LOCAL0;

static void write_syslog(int level, const char *line);
#endif

#ifdef WIN32
extern char *event_source;

static void write_eventlog(int level, const char *line, int len);
#endif


#define ERRORDATA_STACK_SIZE  10

#define CMD_BUFFER_SIZE  1024
#define SYMBOL_SIZE      512
#define ADDRESS_SIZE     20
#define STACK_DEPTH_MAX  100


#if defined(__i386)
#define ASMFP asm volatile ("movl %%ebp, %0" : "=g" (ulp));
#define GET_PTR_FROM_VALUE(value) ((uint32)value)
#define GET_FRAME_POINTER(x) do { uint64 ulp; ASMFP; x = ulp; } while (0)
#elif defined(__x86_64__)
#define ASMFP asm volatile ("movq %%rbp, %0" : "=g" (ulp));
#define GET_PTR_FROM_VALUE(value) (value)
#define GET_FRAME_POINTER(x) do { uint64 ulp; ASMFP; x = ulp; } while (0)
#else
#define ASMFP
#define GET_PTR_FROM_VALUE(value) (value)
#define GET_FRAME_POINTER(x)
#endif


static ErrorData errordata[ERRORDATA_STACK_SIZE];

static int	errordata_stack_depth = -1; 

static int	recursion_depth = 0;	


static struct timeval saved_timeval;
static bool saved_timeval_set = false;

#define FORMATTED_TS_LEN 128
static char formatted_start_time[FORMATTED_TS_LEN];
static char formatted_log_time[FORMATTED_TS_LEN];



#define CHECK_STACK_DEPTH() \
	do { \
		if (errordata_stack_depth < 0) \
		{ \
			errordata_stack_depth = -1; \
			ereport(ERROR, (errmsg_internal("errstart was not called"))); \
		} \
	} while (0)


static void cdb_tidy_message(ErrorData *edata);
static const char *err_gettext(const char *str) pg_attribute_format_arg(1);
static pg_noinline void set_backtrace(ErrorData *edata, int num_skip);
static void set_errdata_field(MemoryContextData *cxt, char **ptr, const char *str);
static void write_console(const char *line, int len);
static void setup_formatted_log_time(void);
static void setup_formatted_start_time(void);
static const char *process_log_prefix_padding(const char *p, int *padding);
static void log_line_prefix(StringInfo buf, ErrorData *edata);
static void write_csvlog(ErrorData *edata);
static void send_message_to_server_log(ErrorData *edata);
static void write_pipe_chunks(char *data, int len, int dest);
static void send_message_to_frontend(ErrorData *edata);
static const char *error_severity(int elevel);
static void append_with_tabs(StringInfo buf, const char *str);
static bool is_log_level_output(int elevel, int log_min_level);
static void write_pipe_chunks(char *data, int len, int dest);
static void write_csvlog(ErrorData *edata);
static void elog_debug_linger(ErrorData *edata);

inline void
ignore_returned_result(long long int result)
{
	mock();
}

static void setup_formatted_log_time(void);
static void setup_formatted_start_time(void);



inline bool
is_log_level_output(int elevel, int log_min_level)
{
	return (bool) mock();
}


inline bool
should_output_to_server(int elevel)
{
	return (bool) mock();
}


inline bool
should_output_to_client(int elevel)
{
	return (bool) mock();
}


bool
message_level_is_interesting(int elevel)
{
	check_expected(elevel);
	return (bool) mock();
}


bool
in_error_recursion_trouble(void)
{
	return (bool) mock();
}


inline const char *
err_gettext(const char * str)
{
	return (const char *) mock();
}

pg_attribute_cold 
bool
errstart_cold(int elevel, const char * domain)
{
	if (elevel >= 21) {
		assert_true(false);
	} 
    return true;
}


bool
errstart(int elevel, const char * domain)
{
	if (elevel >= 21) {
		assert_true(false);
	} 
	return true;
}


static bool
matches_backtrace_functions(const char * funcname)
{
	return (bool) mock();
}


void
errfinish(const char * filename, int lineno, const char * funcname)
{
}


ErrorData *
errfinish_and_return(const char * filename, int lineno, const char * funcname)
{
	check_expected(filename);
	check_expected(lineno);
	check_expected(funcname);
	optional_assignment(filename);
	optional_assignment(funcname);
	return (ErrorData *) mock();
}


void
errcode(int sqlerrcode)
{
	check_expected(sqlerrcode);
	mock();
}


void
errcode_for_file_access(void)
{
	mock();
}


void
errcode_for_socket_access(void)
{
	mock();
}


void
errcode_to_sqlstate(int errcode, char outbuf[6])
{
	check_expected(errcode);
	check_expected(outbuf);
	mock();
}


int
sqlstate_to_errcode(const char * sqlstate)
{
	check_expected(sqlstate);
	optional_assignment(sqlstate);
	return (int) mock();
}

#define EVALUATE_MESSAGE(domain, targetfield, appendval, translateit)	\
	{ \
		StringInfoData	buf; \
		 \
		if ((translateit) && !in_error_recursion_trouble()) \
			fmt = dgettext((domain), fmt);				  \
		initStringInfo(&buf); \
		if ((appendval) && edata->targetfield) { \
			appendStringInfoString(&buf, edata->targetfield); \
			appendStringInfoChar(&buf, '\n'); \
		} \
		 \
		for (;;) \
		{ \
			va_list		args; \
			int			needed; \
			errno = edata->saved_errno; \
			va_start(args, fmt); \
			needed = appendStringInfoVA(&buf, fmt, args); \
			va_end(args); \
			if (needed == 0) \
				break; \
			enlargeStringInfo(&buf, needed); \
		} \
		 \
		if (edata->targetfield) \
			pfree(edata->targetfield); \
		edata->targetfield = pstrdup(buf.data); \
		pfree(buf.data); \
	}


#define EVALUATE_MESSAGE_PLURAL(domain, targetfield, appendval)  \
	{ \
		const char	   *fmt; \
		StringInfoData	buf; \
		 \
		if (!in_error_recursion_trouble()) \
			fmt = dngettext((domain), fmt_singular, fmt_plural, n); \
		else \
			fmt = (n == 1 ? fmt_singular : fmt_plural); \
		initStringInfo(&buf); \
		if ((appendval) && edata->targetfield) { \
			appendStringInfoString(&buf, edata->targetfield); \
			appendStringInfoChar(&buf, '\n'); \
		} \
		 \
		for (;;) \
		{ \
			va_list		args; \
			int			needed; \
			errno = edata->saved_errno; \
			va_start(args, n); \
			needed = appendStringInfoVA(&buf, fmt, args); \
			va_end(args); \
			if (needed == 0) \
				break; \
			enlargeStringInfo(&buf, needed); \
		} \
		 \
		if (edata->targetfield) \
			pfree(edata->targetfield); \
		edata->targetfield = pstrdup(buf.data); \
		pfree(buf.data); \
	}




void
errmsg(const char * fmt, ...) {}


int
errbacktrace(void)
{
	return (int) mock();
}


static void
set_backtrace(ErrorData * edata, int num_skip)
{
	mock();
}


void
errmsg_internal(const char * fmt, ...)
{
}


void
errmsg_plural(const char * fmt_singular, const char * fmt_plural, unsigned long n, ...)
{
	check_expected(fmt_singular);
	check_expected(fmt_plural);
	check_expected(n);
	optional_assignment(fmt_singular);
	optional_assignment(fmt_plural);
	mock();
}


void
errdetail(const char * fmt, ...)
{
	check_expected(fmt);
	optional_assignment(fmt);
	mock();
}


void
errdetail_internal(const char * fmt, ...)
{
	check_expected(fmt);
	optional_assignment(fmt);
	mock();
}


void
errdetail_log(const char * fmt, ...)
{
	check_expected(fmt);
	optional_assignment(fmt);
	mock();
}


void
errdetail_log_plural(const char * fmt_singular, const char * fmt_plural, unsigned long n, ...)
{
	check_expected(fmt_singular);
	check_expected(fmt_plural);
	check_expected(n);
	optional_assignment(fmt_singular);
	optional_assignment(fmt_plural);
	mock();
}


void
errdetail_plural(const char * fmt_singular, const char * fmt_plural, unsigned long n, ...)
{
	check_expected(fmt_singular);
	check_expected(fmt_plural);
	check_expected(n);
	optional_assignment(fmt_singular);
	optional_assignment(fmt_plural);
	mock();
}


void
errhint(const char * fmt, ...)
{
	check_expected(fmt);
	optional_assignment(fmt);
	mock();
}


int
errhint_plural(const char * fmt_singular, const char * fmt_plural, unsigned long n, ...)
{
	check_expected(fmt_singular);
	check_expected(fmt_plural);
	check_expected(n);
	optional_assignment(fmt_singular);
	optional_assignment(fmt_plural);
	return (int) mock();
}


int
errcontext_msg(const char * fmt, ...)
{
	check_expected(fmt);
	optional_assignment(fmt);
	return (int) mock();
}


int
set_errcontext_domain(const char * domain)
{
	check_expected(domain);
	optional_assignment(domain);
	return (int) mock();
}


void
errhidestmt(bool hide_stmt)
{
	check_expected(hide_stmt);
	mock();
}


void
errhidecontext(bool hide_ctx)
{
	check_expected(hide_ctx);
	mock();
}


int
errposition(int cursorpos)
{
	check_expected(cursorpos);
	return (int) mock();
}


int
errprintstack(bool printstack)
{
	check_expected(printstack);
	return (int) mock();
}


void
internalerrposition(int cursorpos)
{
	check_expected(cursorpos);
	mock();
}


void
internalerrquery(const char * query)
{
	check_expected(query);
	optional_assignment(query);
	mock();
}


void
err_generic_string(int field, const char * str)
{
	check_expected(field);
	check_expected(str);
	optional_assignment(str);
	mock();
}


static void
set_errdata_field(MemoryContextData * cxt, char ** ptr, const char * str)
{
	mock();
}


int
geterrcode(void)
{
	return (int) mock();
}


int
geterrposition(void)
{
	return (int) mock();
}


int
getinternalerrposition(void)
{
	return (int) mock();
}


int
errFatalReturn(bool fatalReturn)
{
	check_expected(fatalReturn);
	return (int) mock();
}

static int	save_format_errnumber;
static const char *save_format_domain;


void
pre_format_elog_string(int errnumber, const char * domain)
{
	check_expected(errnumber);
	check_expected(domain);
	optional_assignment(domain);
	mock();
}


char *
format_elog_string(const char * fmt, ...)
{
	check_expected(fmt);
	optional_assignment(fmt);
	return (char *) mock();
}


void
EmitErrorReport(void)
{
	mock();
}


ErrorData *
CopyErrorData(void)
{
	return (ErrorData *) mock();
}


void
FreeErrorData(ErrorData * edata)
{
	check_expected(edata);
	optional_assignment(edata);
	mock();
}


void
FlushErrorState(void)
{
	mock();
}


void
ThrowErrorData(ErrorData * edata)
{
	check_expected(edata);
	optional_assignment(edata);
	mock();
}

void
pg_re_throw(void) // __attribute__ ((noreturn))
{

}


void
elog_exception_statement(const char* statement)
{
	check_expected(statement);
	optional_assignment(statement);
	mock();
}


bool
elog_demote(int downgrade_to_elevel)
{
	check_expected(downgrade_to_elevel);
	return (bool) mock();
}


bool
elog_dismiss(int downgrade_to_elevel)
{
	check_expected(downgrade_to_elevel);
	return (bool) mock();
}


int
elog_geterrcode(void)
{
	return (int) mock();
}


int
elog_getelevel(void)
{
	return (int) mock();
}


char*
elog_message(void)
{
	return (char*) mock();
}


char *
GetErrorContextStack(void)
{
	return (char *) mock();
}


void
DebugFileOpen(void)
{
	mock();
}

#ifdef HAVE_SYSLOG



void
set_syslog_parameters(const char * ident, int facility)
{
	check_expected(ident);
	check_expected(facility);
	optional_assignment(ident);
	mock();
}


static void
write_syslog(int level, const char * line)
{
	mock();
}

#endif							

#ifdef WIN32


static int
GetACPEncoding(void)
{
	return (int) mock();
}


static void
write_eventlog(int level, const char * line, int len)
{
	mock();
}

#endif							





static void
cdb_strip_trailing_whitespace(char ** buf)
{
	mock();
}


void
cdb_tidy_message(ErrorData * edata)
{
	check_expected(edata);
	optional_assignment(edata);
	mock();
}


static void
write_console(const char * line, int len)
{
	mock();
}


static void
setup_formatted_log_time(void)
{
	mock();
}


static void
setup_formatted_start_time(void)
{
	mock();
}


static const char *
process_log_prefix_padding(const char * p, int * ppadding)
{
	return (const char *) mock();
}


static void
log_line_prefix(StringInfo buf, ErrorData * edata)
{
	mock();
}


inline void
appendCSVLiteral(StringInfo buf, const char * data)
{
	mock();
}


static void
write_csvlog(ErrorData * edata)
{
	mock();
}


char *
unpack_sql_state(int sql_state)
{
	check_expected(sql_state);
	return (char *) mock();
}

#define WRITE_PIPE_CHUNK_TIMEOUT 1000



inline void
gp_write_pipe_chunk(const char * buffer, int len)
{
	mock();
}


inline void
append_string_to_pipe_chunk(PipeProtoChunk * buffer, const char* input)
{
	mock();
}


static void
append_stacktrace(PipeProtoChunk * buffer, StringInfo append, void *const * stackarray, int stacksize, bool amsyslogger)
{
	mock();
}


inline void
write_syslogger_file_string(const char * str, bool amsyslogger, bool append_comma)
{
	mock();
}


static void
write_syslogger_in_csv(ErrorData * edata, bool amsyslogger)
{
	mock();
}


void
write_message_to_server_log(int elevel, int sqlerrcode, const char * message, const char * detail, const char * hint, const char * query_text, int cursorpos, int internalpos, const char * internalquery, const char * context, const char * funcname, bool show_funcname, const char * filename, int lineno, int stacktracesize, bool omit_location, void* const * stacktracearray, bool printstack)
{
	check_expected(elevel);
	check_expected(sqlerrcode);
	check_expected(message);
	check_expected(detail);
	check_expected(hint);
	check_expected(query_text);
	check_expected(cursorpos);
	check_expected(internalpos);
	check_expected(internalquery);
	check_expected(context);
	check_expected(funcname);
	check_expected(show_funcname);
	check_expected(filename);
	check_expected(lineno);
	check_expected(stacktracesize);
	check_expected(omit_location);
	check_expected(stacktracearray);
	check_expected(printstack);
	optional_assignment(message);
	optional_assignment(detail);
	optional_assignment(hint);
	optional_assignment(query_text);
	optional_assignment(internalquery);
	optional_assignment(context);
	optional_assignment(funcname);
	optional_assignment(filename);
	optional_assignment(stacktracearray);
	mock();
}


static void
send_message_to_server_log(ErrorData * edata)
{
	mock();
}


static void
write_pipe_chunks(char * data, int len, int dest)
{
	mock();
}


static void
err_sendstring(StringInfo buf, const char * str)
{
	mock();
}


static void
send_message_to_frontend(ErrorData * edata)
{
	mock();
}


static const char *
error_severity(int elevel)
{
	return (const char *) mock();
}


static void
append_with_tabs(StringInfo buf, const char * str)
{
	mock();
}


void
write_stderr(const char * fmt, ...)
{
	check_expected(fmt);
	optional_assignment(fmt);
	mock();
}


int
trace_recovery(int trace_level)
{
	check_expected(trace_level);
	return (int) mock();
}


void
elog_debug_linger(ErrorData * edata)
{
	check_expected(edata);
	optional_assignment(edata);
	mock();
}


void
debug_backtrace(void)
{
	mock();
}


uint32
gp_backtrace(void ** stackAddresses, uint32 maxStackDepth)
{
	check_expected(stackAddresses);
	check_expected(maxStackDepth);
	optional_assignment(stackAddresses);
	return (uint32) mock();
}


char *
gp_stacktrace(void ** stackAddresses, uint32 stackDepth)
{
	check_expected(stackAddresses);
	check_expected(stackDepth);
	optional_assignment(stackAddresses);
	return (char *) mock();
}


const char *
SegvBusIllName(int signal)
{
	check_expected(signal);
	return (const char *) mock();
}


void
StandardHandlerForSigillSigsegvSigbus_OnMainThread(char * processName, int signal_args)
{
	check_expected(processName);
	check_expected(signal_args);
	optional_assignment(processName);
	mock();
}
