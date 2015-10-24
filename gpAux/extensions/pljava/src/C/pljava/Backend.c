/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <miscadmin.h>
#ifndef WIN32
#include <libpq/pqsignal.h>
#endif
#include <executor/spi.h>
#include <commands/trigger.h>
#include <utils/elog.h>
#include <utils/guc.h>
#include <fmgr.h>
#include <access/heapam.h>
#include <utils/syscache.h>
#include <catalog/catalog.h>
#include <catalog/pg_proc.h>
#include <catalog/pg_type.h>
#include <storage/ipc.h>
#include <storage/proc.h>
#include <storage/sinval.h>
#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <pthread.h>

#include "org_postgresql_pljava_internal_Backend.h"
#include "pljava/Invocation.h"
#include "pljava/Function.h"
#include "pljava/HashMap.h"
#include "pljava/Exception.h"
#include "pljava/Backend.h"
#include "pljava/Session.h"
#include "pljava/SPI.h"
#include "pljava/type/String.h"

/* Include the 'magic block' that PostgreSQL 8.2 and up will use to ensure
 * that a module is not loaded into an incompatible server.
 */
#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

extern Datum pljavau_call_handler(PG_FUNCTION_ARGS);
extern Datum pljava_call_handler(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pljava_call_handler);
PG_FUNCTION_INFO_V1(pljavau_call_handler);

#define LOCAL_REFERENCE_COUNT 128

jlong mainThreadId;

static pthread_t s_mainThreadIdForHandler;
static bool s_handlerSubstituted = false;
static pqsigfunc s_oldHandlerFunc = NULL;

static JavaVM* s_javaVM = 0;
static jclass  s_Backend_class;
static jmethodID s_setTrusted;
static bool  s_currentTrust;
static int   s_javaLogLevel;

bool integerDateTimes = false;

extern void Invocation_initialize(void);
extern void Exception_initialize(void);
extern void Exception_initialize2(void);
extern void HashMap_initialize(void);
extern void SPI_initialize(void);
extern void Type_initialize(void);
extern void Function_initialize(void);
extern void Session_initialize(void);
extern void PgSavepoint_initialize(void);
extern void XactListener_initialize(void);
extern void SubXactListener_initialize(void);
extern void SQLInputFromChunk_initialize(void);
extern void SQLOutputToChunk_initialize(void);
extern void SQLInputFromTuple_initialize(void);
extern void SQLOutputToTuple_initialize(void);

static void PLCatchupInterruptHandler(int signo)
{
	if(s_oldHandlerFunc == NULL)
	{
		return;
	}

	pthread_t currentThreadId = pthread_self();

	if(currentThreadId == s_mainThreadIdForHandler)
	{
		s_oldHandlerFunc(signo);
	}
	else
	{
		pthread_kill(s_mainThreadIdForHandler, signo);
	}
}


static void initPLJavaClasses(void)
{
	jfieldID tlField;
	JNINativeMethod backendMethods[] =
	{
		{
		"isCallingJava",
	  	"()Z",
	  	Java_org_postgresql_pljava_internal_Backend_isCallingJava
		},
		{
		"isReleaseLingeringSavepoints",
	  	"()Z",
	  	Java_org_postgresql_pljava_internal_Backend_isReleaseLingeringSavepoints
		},
		{
		"_getConfigOption",
		"(Ljava/lang/String;)Ljava/lang/String;",
		Java_org_postgresql_pljava_internal_Backend__1getConfigOption
		},
		{
		"_getLibraryPath",
		"()Ljava/lang/String;",
		Java_org_postgresql_pljava_internal_Backend__1getLibraryPath
		},
		{
		"_getStatementCacheSize",
		"()I",
		Java_org_postgresql_pljava_internal_Backend__1getStatementCacheSize
		},
		{
		"_log",
		"(ILjava/lang/String;)V",
		Java_org_postgresql_pljava_internal_Backend__1log
		},
		{
		"_clearFunctionCache",
		"()V",
		Java_org_postgresql_pljava_internal_Backend__1clearFunctionCache
		},
		{ 0, 0, 0 }
	};

	Exception_initialize();

	elog(DEBUG1, "Getting Backend class pljava.jar");
	s_Backend_class = PgObject_getJavaClass("org/postgresql/pljava/internal/Backend");
	elog(DEBUG1, "Backend class was there");
	PgObject_registerNatives2(s_Backend_class, backendMethods);

	tlField = PgObject_getStaticJavaField(s_Backend_class, "THREADLOCK", "Ljava/lang/Object;");
	JNI_setThreadLock(JNI_getStaticObjectField(s_Backend_class, tlField));

	Invocation_initialize();
	Exception_initialize2();
	SPI_initialize();
	Type_initialize();
	Function_initialize();
	Session_initialize();
	PgSavepoint_initialize();
	XactListener_initialize();
	SubXactListener_initialize();
	SQLInputFromChunk_initialize();
	SQLOutputToChunk_initialize();
	SQLInputFromTuple_initialize();
	SQLOutputToTuple_initialize();

	s_setTrusted = PgObject_getStaticJavaMethod(s_Backend_class, "setTrusted", "(Z)V");
}

/**
 *  Initialize security
 */
void Backend_setJavaSecurity(bool trusted)
{
	if(trusted != s_currentTrust)
	{
		/* GCJ has major issues here. Real work on SecurityManager and
		 * related classes has just started in version 4.0.0.
		 */
#ifndef GCJ
		JNI_callStaticVoidMethod(s_Backend_class, s_setTrusted, (jboolean)trusted);
		if(JNI_exceptionCheck())
		{
			JNI_exceptionDescribe();
			JNI_exceptionClear();
			ereport(ERROR, (
				errcode(ERRCODE_INTERNAL_ERROR),
				errmsg("Unable to initialize java security")));
		}
#endif
		s_currentTrust = trusted;
	}
}

int Backend_setJavaLogLevel(int logLevel)
{
	int oldLevel = s_javaLogLevel;
	s_javaLogLevel = logLevel;
	return oldLevel;
}

/**
 * Special purpose logging function called from JNI when verbose is enabled.
 */
static jint JNICALL my_vfprintf(FILE* fp, const char* format, va_list args)
{
	char buf[1024];
	char* ep;
	char* bp = buf;

    vsnprintf(buf, sizeof(buf), format, args);

    /* Trim off trailing newline and other whitespace.
     */
	ep = bp + strlen(bp) - 1;
    while(ep >= bp && isspace(*ep))
 		--ep;
 	++ep;
 	*ep = 0;

    elog(s_javaLogLevel, "%s", buf);
    return 0;
}

/*
 * Append those parts of path that has not yet been appended. The HashMap unique is
 * keeping track of what has been appended already. First appended part will be
 * prefixed with prefix.
 */
static void appendPathParts(const char* path, StringInfoData* bld, HashMap unique, const char* prefix)
{
	StringInfoData buf;
	if(path == 0 || strlen(path) == 0)
		return;

	for (;;)
	{
		char* pathPart;
		size_t len;
		if(*path == 0)
			break;

		len = strcspn(path, ";:");

		if(len == 1 && *(path+1) == ':' && isalnum(*path))
			/*
			 * Windows drive designator, leave it "as is".
			 */
			len = strcspn(path+2, ";:") + 2;
		else
		if(len == 0)
			{
			/* Ignore zero length components.
			 */
			++path;
			continue;
			}

		initStringInfo(&buf);
		if(*path == '$')
		{
			if(len == 7 || (strcspn(path, "/\\") == 7 && strncmp(path, "$libdir", 7) == 0))
			{
				len -= 7;
				path += 7;
				appendStringInfo(&buf, "%s", pkglib_path);
			}
			else
				ereport(ERROR, (
					errcode(ERRCODE_INVALID_NAME),
					errmsg("invalid macro name '%*s' in dynamic library path", (int)len, path)));
		}

		if(len > 0)
		{
			appendBinaryStringInfo(&buf, path, len);
			path += len;
		}

		pathPart = buf.data;
		if(HashMap_getByString(unique, pathPart) == 0)
		{
			if(HashMap_size(unique) == 0)
				appendStringInfo(bld, "%s", prefix);
			else
#if defined(WIN32)
				appendStringInfoChar(bld, ';');
#else
				appendStringInfoChar(bld, ':');
#endif
			appendStringInfo(bld, "%s", pathPart);
			HashMap_putByString(unique, pathPart, (void*)1);
		}
		pfree(pathPart);
		if(*path == 0)
			break;
		++path; /* Skip ':' */
	}
}

/*
 * Get the CLASSPATH. Result is always freshly palloc'd.
 */
static char* getClassPath(const char* prefix)
{
	char* path;
	HashMap unique = HashMap_create(13, CurrentMemoryContext);
	StringInfoData buf;
	initStringInfo(&buf);


	/* Put the pljava installed in the $libdir first in the path */
	appendPathParts("$libdir/pljava.jar", &buf, unique, prefix);

#if 0
	/*
	 * Currently pljava.classpath is user setable, which makes this a
	 * security problem.  If CLASSPATH needs to be setable beyond simply
	 * locating the pljava.jar file then this requires modification.
	 *
	 * The Greenplum version of pljava currently uses the classpath guc
	 * differently anyhow due to differences in storing the jar files
	 * in the filesystem rather than in the database.
	 */
	appendPathParts(pljava_classpath, &buf, unique, prefix);

	/*
	 * For this to be useful it needs to be propagated from the
	 * master to all the segments, otherwise it wouldn't be the
	 * same everyplace and that would be a problem.
	 *
	 * Using a jvm_classpath GUC makes more architectural sense,
	 * for it to be secure it would need to be super-user only,
	 * possibly conf file only.
	 */
	appendPathParts(getenv("CLASSPATH"), &buf, unique, prefix);
#endif

	PgObject_free((PgObject)unique);
	path = buf.data;
	if(strlen(path) == 0)
	{
		pfree(path);
		path = 0;
	}
	return path;
}

#if !defined(WIN32)

static void pljavaStatementCancelHandler(int signum)
{
	if(!proc_exit_inprogress)
	{
		/* Never service the interrupt immediately. In order to find out if
		 * its safe, we would need to know what kind of threading mechanism
		 * the VM uses. That would count for a lot of conditional code.
		 */
		QueryCancelPending = true;
		InterruptPending = true;
	}
}

static void pljavaDieHandler(int signum)
{
	if(!proc_exit_inprogress)
	{
		/* Never service the interrupt immediately. In order to find out if
		 * its safe, we would need to know what kind of threading mechanism
		 * the VM uses. That would count for a lot of conditional code.
		 */
		ProcDiePending = true;
		InterruptPending = true;
	}
}

static sigjmp_buf recoverBuf;
static void terminationTimeoutHandler(int signum)
{
	kill(MyProcPid, SIGQUIT);

	/* Some sleep to get the SIGQUIT a chance to generate
	 * the needed output.
	 */
	pg_usleep(1);

	/* JavaVM did not die within the alloted time
	 */
	siglongjmp(recoverBuf, 1);
}
#endif

/*
 * proc_exit callback to tear down the JVM
 */
static void _destroyJavaVM(int status, Datum dummy)
{
	if(s_javaVM != 0)
	{
		Invocation ctx;
#if !defined(WIN32)
		pqsigfunc saveSigAlrm;

		Invocation_pushInvocation(&ctx, false);
		if(sigsetjmp(recoverBuf, 1) != 0)
		{
			elog(DEBUG1, "JavaVM destroyed with force");
			s_javaVM = 0;
			return;
		}

		saveSigAlrm = pqsignal(SIGALRM, terminationTimeoutHandler);
		enable_sig_alarm(5000, false);

		elog(DEBUG1, "Destroying JavaVM...");
		JNI_destroyVM(s_javaVM);
		disable_sig_alarm(false);
		pqsignal(SIGALRM, saveSigAlrm);
#else
		Invocation_pushInvocation(&ctx, false);
		elog(DEBUG1, "Destroying JavaVM...");
		JNI_destroyVM(s_javaVM);
#endif
		elog(DEBUG1, "JavaVM destroyed");
		s_javaVM = 0;
		currentInvocation = 0;

		/*
		 * Recover the handler of SIGUSR1 when initiate JVM failed.
		 * */
		if(s_oldHandlerFunc == NULL)
		{
			pqsignal(SIGUSR1, SIG_IGN);
		}
		else
		{
			pqsignal(SIGUSR1, s_oldHandlerFunc);
		}
		s_handlerSubstituted = false;
	}
}

typedef struct {
	JavaVMOption* options;
	unsigned int  size;
	unsigned int  capacity;
} JVMOptList;

static void JVMOptList_init(JVMOptList* jol)
{
	jol->options  = (JavaVMOption*)palloc(10 * sizeof(JavaVMOption));
	jol->size     = 0;
	jol->capacity = 10;
}

static void JVMOptList_delete(JVMOptList* jol)
{
	JavaVMOption* opt = jol->options;
	JavaVMOption* top = opt + jol->size;
	while(opt < top)
	{
		pfree(opt->optionString);
		opt++;
	}
	pfree(jol->options);
}

static void JVMOptList_add(JVMOptList* jol, const char* optString, void* extraInfo, bool mustCopy)
{
	JavaVMOption* added;

	int newPos = jol->size;
	if(newPos >= jol->capacity)
	{
		int newCap = jol->capacity * 2;
		JavaVMOption* newOpts = (JavaVMOption*)palloc(newCap * sizeof(JavaVMOption));
		memcpy(newOpts, jol->options, newPos * sizeof(JavaVMOption));
		pfree(jol->options);
		jol->options = newOpts;
		jol->capacity = newCap;
	}
	added = jol->options + newPos;
	if(mustCopy)
		optString = pstrdup(optString);

	added->optionString = (char*)optString;
	added->extraInfo    = extraInfo;
	jol->size++;

	elog(DEBUG1, "Added JVM option string \"%s\"", optString);
}

/* Split JVM options. The string is split on whitespace unless the
 * whitespace is found within a string or is escaped by backslash. A
 * backslash escaped quote is not considered a string delimiter.
 */
static void addUserJVMOptions(JVMOptList* optList)
{
	const char* cp = pljava_vmoptions;

	if(cp != NULL)
	{
		StringInfoData buf;
		char quote = 0;
		char c;

		initStringInfo(&buf);
		for(;;)
		{
			c = *cp++;
			switch(c)
			{
				case 0:
					break;

				case '"':
				case '\'':
					if(quote == c)
						quote = 0;
					else
						quote = c;
					appendStringInfoChar(&buf, c);
					continue;

				case '\\':
					appendStringInfoChar(&buf, '\\');
					c = *cp++;	/* Interpret next character verbatim */
					if(c == 0)
						break;
					appendStringInfoChar(&buf, c);
					continue;

				default:
					if(quote == 0 && isspace((int)c))
					{
						while((c = *cp++) != 0)
						{
							if(!isspace((int)c))
								break;
						}

						if(c == 0)
							break;

						if(c != '-')
							appendStringInfoChar(&buf, ' ');
						else if(buf.len > 0)
						{
							/* Whitespace followed by '-' triggers new
							 * option declaration.
							 */
							JVMOptList_add(optList, buf.data, 0, true);
							buf.len = 0;
							buf.data[0] = 0;
						}
					}
					appendStringInfoChar(&buf, c);
					continue;
			}
			break;
		}
		if(buf.len > 0)
			JVMOptList_add(optList, buf.data, 0, true);
		pfree(buf.data);
	}
}

/**
 *  Initialize the session
 */
static void initJavaSession(void)
{
	jclass sessionClass = PgObject_getJavaClass("org/postgresql/pljava/internal/Session");
	jmethodID init = PgObject_getStaticJavaMethod(sessionClass, "init", "()J");
	mainThreadId = JNI_callStaticLongMethod(sessionClass, init);
	JNI_deleteLocalRef(sessionClass);

	if(JNI_exceptionCheck())
	{
		JNI_exceptionDescribe();
		JNI_exceptionClear();
		ereport(ERROR, (
			errcode(ERRCODE_INTERNAL_ERROR),
			errmsg("Unable to initialize java session")));
	}
}

static void checkIntTimeType(void)
{
	const char* idt = GetConfigOption("integer_datetimes");
	integerDateTimes = (strcmp(idt, "on") == 0);
	elog(DEBUG1, integerDateTimes ? "Using integer_datetimes" : "Not using integer_datetimes");
}

static char *get_jni_errmsg(jint jnicode)
{
	switch (jnicode)
	{
		case -1:        return "unknown error";
		case -2:        return "thread detached from the VM";
		case -3:        return "JNI version error";
		case -4:        return "not enough memory";
		case -5:        return "VM already created";
		case -6:        return "invalid arguments";
		default:        return "unknown error";
	}
}

static bool s_firstTimeInit = true;

static void initializeJavaVM(void)
{
	jint jstat;
	JavaVMInitArgs vm_args;
	JVMOptList optList;

	JVMOptList_init(&optList);

	if(s_firstTimeInit)
	{
		s_firstTimeInit = false;
		s_javaLogLevel = INFO;

		checkIntTimeType();
		HashMap_initialize();

		s_firstTimeInit = false;
	}

#ifdef PLJAVA_DEBUG
	/* Hard setting for debug. Don't forget to recompile...
	 */
	pljava_debug = 1;
#endif

	addUserJVMOptions(&optList);
	effectiveClassPath = getClassPath("-Djava.class.path=");
	if(effectiveClassPath != 0)
	{
		JVMOptList_add(&optList, effectiveClassPath, 0, true);
	}

	/**
	 * As stipulated by JRT-2003
	 */
	JVMOptList_add(&optList,
		"-Dsqlj.defaultconnection=jdbc:default:connection",
		0, true);

	JVMOptList_add(&optList, "vfprintf", (void*)my_vfprintf, true);
#ifndef GCJ
	JVMOptList_add(&optList, "-Xrs", 0, true);
#endif
	if(pljava_debug)
	{
		elog(INFO, "Backend pid = %d. Attach the debugger and set pljava_debug to false to continue", getpid());
		while(pljava_debug)
			pg_usleep(1000000L);
	}

	vm_args.nOptions = optList.size;
	vm_args.options  = optList.options;
	vm_args.version  = JNI_VERSION_1_4;
	vm_args.ignoreUnrecognized = JNI_FALSE;

	elog(DEBUG1, "Creating JavaVM");

	jstat = JNI_createVM(&s_javaVM, &vm_args);

	if(jstat == JNI_OK && JNI_exceptionCheck())
	{
		JNI_exceptionDescribe();
		JNI_exceptionClear();
		jstat = JNI_ERR;
	}
	JVMOptList_delete(&optList);

	if(jstat != JNI_OK)
	{
		elog(ERROR, "Failed to create Java VM: %s", get_jni_errmsg(jstat));
	}

#if !defined(WIN32)
	pqsignal(SIGINT,  pljavaStatementCancelHandler);
	pqsignal(SIGTERM, pljavaDieHandler);
#endif
	elog(DEBUG1, "JavaVM created");

	/* Register an on_proc_exit handler that destroys the VM
	 */
	on_proc_exit(_destroyJavaVM, 0);
	initPLJavaClasses();
	initJavaSession();
}

static Datum internalCallHandler(bool trusted, PG_FUNCTION_ARGS);


/*
 * This is the entry point for all untrusted calls.
 */
Datum pljavau_call_handler(PG_FUNCTION_ARGS)
{
	return internalCallHandler(false, fcinfo);
}


/*
 * This is the entry point for all trusted calls.
 */
Datum pljava_call_handler(PG_FUNCTION_ARGS)
{
	return internalCallHandler(true, fcinfo);
}

static Datum internalCallHandler(bool trusted, PG_FUNCTION_ARGS)
{
	Invocation ctx;
	Datum retval = 0;


	/*
	 * Substitute SIGUSR1 handler CatchupInterruptHandler with new local
	 * handler PLCatchupInterruptHandler, which will strictly let main
	 * thread of GPDB to process the signal rather than any other threads.
	 *
	 * Resolving possible lwlock releasing crash caused by SIGUSR1.
	 * MPP-23735
	 */
	if(!s_handlerSubstituted)
	{
		s_mainThreadIdForHandler = pthread_self();
		s_oldHandlerFunc = pqsignal(SIGUSR1, PLCatchupInterruptHandler);
		s_handlerSubstituted = true;
	}

	if(s_javaVM == 0)
	{
		Invocation_pushBootContext(&ctx);
		PG_TRY();
		{
			initializeJavaVM();
			Invocation_popBootContext();
		}
		PG_CATCH();
		{
			Invocation_popBootContext();

			/* JVM initialization failed for some reason. Destroy
			 * the VM if it exists. Perhaps the user will try
			 * fixing the pljava_classpath and make a new attempt.
			 */
			_destroyJavaVM(0, 0);

			/* We can't stay here...
			 */
			PG_RE_THROW();
		}
		PG_END_TRY();

		/* Force initial setting
 		 */
		s_currentTrust = !trusted;
	}

	Invocation_pushInvocation(&ctx, trusted);
	PG_TRY();
	{
		Function function = Function_getFunction(fcinfo);
		if(CALLED_AS_TRIGGER(fcinfo))
		{
			/* Called as a trigger procedure
			 */
			retval = Function_invokeTrigger(function, fcinfo);
		}
		else
		{
			/* Called as a function
			 */
			retval = Function_invoke(function, fcinfo);
		}
		Invocation_popInvocation(false);
	}
	PG_CATCH();
	{
		Invocation_popInvocation(true);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return retval;
}

/****************************************
 * JNI methods
 ****************************************/
JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void* reserved)
{
	return JNI_VERSION_1_4;
}

/*
 * Class:     org_postgresql_pljava_internal_Backend
 * Method:    _getConfigOption
 * Signature: (Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
JNICALL Java_org_postgresql_pljava_internal_Backend__1getConfigOption(JNIEnv* env, jclass cls, jstring jkey)
{
	jstring result = 0;

	BEGIN_NATIVE
	char* key = String_createNTS(jkey);
	if(key != 0)
	{
		PG_TRY();
		{
			const char* value = GetConfigOption(key);
			pfree(key);
			if(value != 0)
				result = String_createJavaStringFromNTS(value);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("GetConfigOption");
		}
		PG_END_TRY();
	}
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_Backend
 * Method:    _getLibraryPath
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL
JNICALL Java_org_postgresql_pljava_internal_Backend__1getLibraryPath(JNIEnv* env, jclass cls)
{
	jstring result = 0;

	BEGIN_NATIVE
	PG_TRY();
    {
		result = String_createJavaStringFromNTS(pkglib_path);
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("GetLibraryPath");
	}
	PG_END_TRY();
	END_NATIVE
	return result;
}


/*
 * Class:     org_postgresql_pljava_internal_Backend
 * Method:    _getStatementCacheSize
 * Signature: ()I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_Backend__1getStatementCacheSize(JNIEnv* env, jclass cls)
{
	return pljava_statement_cache_size;
}

/*
 * Class:     org_postgresql_pljava_internal_Backend
 * Method:    _log
 * Signature: (ILjava/lang/String;)V
 */
JNIEXPORT void JNICALL
JNICALL Java_org_postgresql_pljava_internal_Backend__1log(JNIEnv* env, jclass cls, jint logLevel, jstring jstr)
{
	BEGIN_NATIVE_NO_ERRCHECK
	char* str = String_createNTS(jstr);
	if(str != 0)
	{
		PG_TRY();
		{
			elog(logLevel, "%s", str);
			pfree(str);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("ereport");
		}
		PG_END_TRY();
	}
	END_NATIVE
}

/*
 * Class:     org_postgresql_pljava_internal_Backend
 * Method:    isCallingJava
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL
Java_org_postgresql_pljava_internal_Backend_isCallingJava(JNIEnv* env, jclass cls)
{
	return JNI_isCallingJava();
}

/*
 * Class:     org_postgresql_pljava_internal_Backend
 * Method:    isReleaseLingeringSavepoints
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL
Java_org_postgresql_pljava_internal_Backend_isReleaseLingeringSavepoints(JNIEnv* env, jclass cls)
{
	return pljava_release_lingering_savepoints ? JNI_TRUE : JNI_FALSE;
}

/*
 * Class:     org_postgresql_pljava_internal_Backend
 * Method:    _clearFunctionCache
 * Signature: ()V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_Backend__1clearFunctionCache(JNIEnv* env, jclass cls)
{
	BEGIN_NATIVE_NO_ERRCHECK
	Function_clearFunctionCache();
	END_NATIVE
}
