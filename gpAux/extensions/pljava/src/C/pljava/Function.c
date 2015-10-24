/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "pljava/PgObject_priv.h"
#include "pljava/Exception.h"
#include "pljava/Invocation.h"
#include "pljava/Function.h"
#include "pljava/HashMap.h"
#include "pljava/Iterator.h"
#include "pljava/type/Oid.h"
#include "pljava/type/String.h"
#include "pljava/type/TriggerData.h"
#include "pljava/type/UDT.h"

#include <catalog/pg_proc.h>
#include <catalog/pg_namespace.h>
#include <utils/builtins.h>
#include <ctype.h>
#include <funcapi.h>
#include <utils/typcache.h>

#define PARAM_OIDS(procStruct) (procStruct)->proargtypes.values

static jclass s_Loader_class;
static jclass s_ClassLoader_class;
static jmethodID s_Loader_getSchemaLoader;
static jmethodID s_Loader_getTypeMap;
static jmethodID s_ClassLoader_loadClass;
static PgObjectClass s_FunctionClass;

struct Function_
{
	struct PgObject_ PgObject_extension;

	/**
	 * True if the function is not a volatile function (i.e. STABLE or
	 * IMMUTABLE). This means that the function is not allowed to have
	 * side effects.
	 */
	bool   readOnly;

	/**
	 * True if this is a UDT function (input/output/receive/send)
	 */
	bool   isUDT;

	/**
	 * Java class, i.e. the UDT class or the class where the static method
	 * is defined.
	 */
	jclass clazz;

	union
	{
		struct
		{
		/*
		 * True if the function is a multi-call function and hence, will
		 * allocate a memory context of its own.
		 */
		bool      isMultiCall;
	
		/*
		 * The number of parameters
		 */
		int32     numParams;
	
		/*
		 * Array containing one type for eeach parameter.
		 */
		Type*     paramTypes;
	
		/*
		 * The return type.
		 */
		Type      returnType;

		/*
		 * The type map used when mapping parameter and return types. We
		 * need to store it here in order to cope with dynamic types (any
		 * and anyarray)
		 */
		jobject typeMap;

		/*
		 * The static method that should be called.
		 */
		jmethodID method;
		} nonudt;
		
		struct
		{
		/**
		 * The UDT that this function is associated with
		 */
		UDT udt;

		/**
		 * The UDT function to call
		 */
		UDTFunction udtFunction;
		} udt;
	} func;
};

typedef struct ParseResultData
{
	char* buffer;	/* The buffer to pfree once we are done */
	const char* returnType;
	const char* className;
	const char* methodName;
	const char* parameters;
	bool isUDT;
} ParseResultData;

typedef ParseResultData *ParseResult;

static HashMap s_funcMap = 0;

static jclass s_Loader_class;
static jmethodID s_Loader_getSchemaLoader;

static void _Function_finalize(PgObject func)
{
	Function self = (Function)func;
	JNI_deleteGlobalRef(self->clazz);
	if(!self->isUDT)
	{
		if(self->func.nonudt.typeMap != 0)
			JNI_deleteGlobalRef(self->func.nonudt.typeMap);
		if(self->func.nonudt.paramTypes != 0)
			pfree(self->func.nonudt.paramTypes);
	}
}

extern void Function_initialize(void);
void Function_initialize(void)
{
	s_funcMap = HashMap_create(59, TopMemoryContext);
	
	s_Loader_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/sqlj/Loader"));
	s_Loader_getSchemaLoader = PgObject_getStaticJavaMethod(s_Loader_class, "getSchemaLoader", "(Ljava/lang/String;)Ljava/lang/ClassLoader;");
	s_Loader_getTypeMap = PgObject_getStaticJavaMethod(s_Loader_class, "getTypeMap", "(Ljava/lang/String;)Ljava/util/Map;");

	s_ClassLoader_class = JNI_newGlobalRef(PgObject_getJavaClass("java/lang/ClassLoader"));
	s_ClassLoader_loadClass = PgObject_getJavaMethod(s_ClassLoader_class, "loadClass", "(Ljava/lang/String;)Ljava/lang/Class;");

	s_FunctionClass  = PgObjectClass_create("Function", sizeof(struct Function_), _Function_finalize);
}

static void buildSignature(Function self, StringInfo sign, Type retType, bool alt)
{
	Type* tp = self->func.nonudt.paramTypes;
	Type* ep = tp + self->func.nonudt.numParams;

	appendStringInfoChar(sign, '(');
	while(tp < ep)
		appendStringInfoString(sign, Type_getJNISignature(*tp++));

	if(!self->func.nonudt.isMultiCall && Type_isOutParameter(retType))
		appendStringInfoString(sign, Type_getJNISignature(retType));

	appendStringInfoChar(sign, ')');
	appendStringInfoString(sign, Type_getJNIReturnSignature(retType, self->func.nonudt.isMultiCall, alt));
}

static void parseParameters(Function self, Oid* dfltIds, const char* paramDecl)
{
	char c;
	int idx = 0;
	int top = self->func.nonudt.numParams;
	bool lastIsOut = !self->func.nonudt.isMultiCall && Type_isOutParameter(self->func.nonudt.returnType);
	StringInfoData sign;
	initStringInfo(&sign);
	for(;;)
	{
		if(idx >= top)
		{
			if(!(lastIsOut && idx == top))
				ereport(ERROR, (
					errcode(ERRCODE_SYNTAX_ERROR),
					errmsg("To many parameters - expected %d ", top)));
		}

		c = *paramDecl++;
		if(c == 0 || c == ',')
		{
			Type deflt = (idx == top) ? self->func.nonudt.returnType : self->func.nonudt.paramTypes[idx];
			const char* jtName = Type_getJavaTypeName(deflt);
			if(strcmp(jtName, sign.data) != 0)
			{
				Oid did;
				Type repl;
				if(idx == top)
					/*
					 * Last parameter is the OUT parameter. It has no corresponding
					 * entry in the dfltIds array.
					 */
					did = InvalidOid;
				else
					did = dfltIds[idx];

				repl = Type_fromJavaType(did, sign.data);
				if(!Type_canReplaceType(repl, deflt))
					repl = Type_getCoerceIn(repl, deflt);

				if(idx == top)
					self->func.nonudt.returnType = repl;
				else
					self->func.nonudt.paramTypes[idx] = repl;
			}
			pfree(sign.data);

			++idx;
			if(c == 0)
			{
				/*
				 * We are done.
				 */
				if(lastIsOut)
					++top;
				if(idx != top)
					ereport(ERROR, (
						errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("To few parameters - expected %d ", top)));
				break;
			}

			/*
			 * Initialize next parameter.
			 */
			initStringInfo(&sign);
		}
		else
			appendStringInfoChar(&sign, c);
	}
}

static char* getAS(HeapTuple procTup, char** epHolder)
{
	char c;
	char* cp1;
	char* cp2;
	char* bp;
	bool  atStart = true;
	bool  passedFirst = false;
	bool  isNull = false;
	Datum tmp = SysCacheGetAttr(PROCOID, procTup, Anum_pg_proc_prosrc, &isNull);
	if(isNull)
	{
		ereport(ERROR, (
			errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("'AS' clause of Java function cannot be NULL")));
	}

	bp = pstrdup(DatumGetCString(DirectFunctionCall1(textout, tmp)));

	/* Strip all whitespace except the first one if it occures after
	 * some alpha numeric characers and before some other alpha numeric
	 * characters. We insert a '=' when that happens since it delimits
	 * the return value from the method name.
	 */
	cp1 = cp2 = bp;
	while((c = *cp1++) != 0)
	{
		if(isspace(c))
		{
			if(atStart || passedFirst)
				continue;

			while((c = *cp1++) != 0)
				if(!isspace(c))
					break;

			if(c == 0)
				break;

			if(isalpha(c))
				*cp2++ = '=';
			passedFirst = true;
		}
		atStart = false;
		if(!isalnum(c))
			passedFirst = true;
		*cp2++ = c;
	}
	*cp2 = 0;
	*epHolder = cp2;
	return bp;
}

static void parseUDT(ParseResult info, char* bp, char* ep)
{
	char* ip = ep - 1;
	while(ip > bp && *ip != ']')
		--ip;

	if(ip == bp)
	{
		ereport(ERROR, (
			errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("Missing ending ']' in UDT declaration")));
	}
	*ip = 0; /* Terminate class name */
	info->className = bp;
	info->methodName = ip + 1;
	info->isUDT = true;
}

static void parseFunction(ParseResult info, HeapTuple procTup)
{
	/* The user's function definition must be the fully
	 * qualified name of a java method short of parameter
	 * signature.
	 */
	char* ip;
	char* ep;
	char* bp = getAS(procTup, &ep);

	info->buffer = bp;

	/* The AS clause can have two formats
	 *
	 * <class name> "." <method name> [ "(" <parameter decl> ["," <parameter decl> ... ] ")" ]
	 *   or
	 * "UDT" "[" <class name> "]" <UDT function type>
	 * where <UDT function type> is one of "input", "output", "receive" or "send"
	 */
	if(ep - bp >= 4 && strncasecmp(bp, "udt[", 4) == 0)
	{
		parseUDT(info, bp + 4, ep);
		return;
	}

	info->isUDT = false;

	/* Scan backwards from ep.
	 */
	ip = ep - 1;
	if(*ip == ')')
	{
		/* We have an explicit parameter type declaration
		 */
		*ip-- = 0;
		while(ip > bp && *ip != '(')
			--ip;

		if(ip == bp)
		{
			ereport(ERROR, (
				errcode(ERRCODE_SYNTAX_ERROR),
				errmsg("Unbalanced parenthesis")));
		}

		info->parameters = ip + 1;
		*ip-- = 0;
	}

	/* Find last '.' occurrence.
	*/
	while(ip > bp && *ip != '.')
		--ip;

	if(ip == bp)
	{
		ereport(ERROR, (
			errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("Did not find <fully qualified class>.<method name>")));
	}
	info->methodName = ip + 1;
	*ip = 0;
	
	/* Check if we have a return type declaration
	 */
	while(--ip > bp)
	{
		if(*ip == '=')
		{
			info->className = ip + 1;
			*ip = 0;
			break;
		}
	}

	if(info->className != 0)
		info->returnType = bp;
	else
		info->className = bp;

	elog(DEBUG3, "className = '%s', methodName = '%s', parameters = '%s', returnType = '%s'",
		info->className == 0 ? "null" : info->className,
		info->methodName == 0 ? "null" : info->methodName,
		info->parameters == 0 ? "null" : info->parameters,
		info->returnType == 0 ? "null" : info->returnType);
}

static jstring getSchemaName(int namespaceOid)
{
	HeapTuple nspTup = PgObject_getValidTuple(NAMESPACEOID, namespaceOid, "namespace");
	Form_pg_namespace nspStruct = (Form_pg_namespace)GETSTRUCT(nspTup);
	jstring schemaName = String_createJavaStringFromNTS(NameStr(nspStruct->nspname));
	ReleaseSysCache(nspTup);
	return schemaName;
}

static void setupTriggerParams(Function self, ParseResult info)
{
	if(info->parameters != 0)
		ereport(ERROR, (
			errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("Triggers can not have a java parameter declaration")));

	self->func.nonudt.returnType = Type_fromJavaType(InvalidOid, "void");

	/* Parameters are not used when calling triggers.
		*/
	self->func.nonudt.numParams  = 1;
	self->func.nonudt.paramTypes = (Type*)MemoryContextAlloc(GetMemoryChunkContext(self), sizeof(Type));
	self->func.nonudt.paramTypes[0] = Type_fromJavaType(InvalidOid, "org.postgresql.pljava.TriggerData");
}

static void setupUDT(Function self, ParseResult info, Form_pg_proc procStruct)
{
	Oid udtId = 0;
	HeapTuple typeTup;
	Form_pg_type pgType;

	if(strcasecmp("input", info->methodName) == 0)
	{
		self->func.udt.udtFunction = UDT_input;
		udtId = procStruct->prorettype;
	}
	else if(strcasecmp("output", info->methodName) == 0)
	{
		self->func.udt.udtFunction = UDT_output;
		udtId = PARAM_OIDS(procStruct)[0];
	}
	else if(strcasecmp("receive", info->methodName) == 0)
	{
		self->func.udt.udtFunction = UDT_receive;
		udtId = procStruct->prorettype;
	}
	else if(strcasecmp("send", info->methodName) == 0)
	{
		self->func.udt.udtFunction = UDT_send;
		udtId = PARAM_OIDS(procStruct)[0];
	}
	else
	{
		ereport(ERROR, (
			errcode(ERRCODE_SYNTAX_ERROR),
			errmsg("Unknown UDT function %s", info->methodName)));
	}

	typeTup = PgObject_getValidTuple(TYPEOID, udtId, "type");
	pgType = (Form_pg_type)GETSTRUCT(typeTup);
	self->func.udt.udt = UDT_registerUDT(self->clazz, udtId, pgType, 0, true);
	ReleaseSysCache(typeTup);
}

static void setupFunctionParams(Function self, ParseResult info, Form_pg_proc procStruct, PG_FUNCTION_ARGS)
{
	Oid* paramOids;
	MemoryContext ctx = GetMemoryChunkContext(self);
	int32 top = (int32)procStruct->pronargs;;

	self->func.nonudt.numParams = top;
	self->func.nonudt.isMultiCall = procStruct->proretset;
	self->func.nonudt.returnType = Type_fromOid(procStruct->prorettype, self->func.nonudt.typeMap);

	if(top > 0)
	{
		int idx;
		paramOids = PARAM_OIDS(procStruct);
		self->func.nonudt.paramTypes = (Type*)MemoryContextAlloc(ctx, top * sizeof(Type));

		for(idx = 0; idx < top; ++idx)
			self->func.nonudt.paramTypes[idx] = Type_fromOid(paramOids[idx], self->func.nonudt.typeMap);
	}
	else
	{
		self->func.nonudt.paramTypes = 0;
		paramOids = 0;
	}

	if(info->parameters != 0)
		parseParameters(self, paramOids, info->parameters);

	if(info->returnType != 0)
	{
		const char* jtName = Type_getJavaTypeName(self->func.nonudt.returnType);
		if(strcmp(jtName, info->returnType) != 0)
		{
			Type repl = Type_fromJavaType(Type_getOid(self->func.nonudt.returnType), info->returnType);
			if(!Type_canReplaceType(repl, self->func.nonudt.returnType))
				repl = Type_getCoerceOut(repl, self->func.nonudt.returnType);
			self->func.nonudt.returnType = repl;
		}
	}
}

static void Function_init(Function self, ParseResult info, Form_pg_proc procStruct, PG_FUNCTION_ARGS)
{
	StringInfoData sign;
	jobject loader;
	jstring className;

	/* Get the ClassLoader for the schema that this function belongs to
	 */
	jstring schemaName = getSchemaName(procStruct->pronamespace);

	/* Install the type map for the current schema. This must be done ASAP since
	 * many other functions (including obtaining the loader) depends on it.
	 */
	jobject tmp = JNI_callStaticObjectMethod(s_Loader_class, s_Loader_getTypeMap, schemaName);
	self->func.nonudt.typeMap = JNI_newGlobalRef(tmp);
	JNI_deleteLocalRef(tmp);

	self->readOnly = (procStruct->provolatile != PROVOLATILE_VOLATILE);
	self->isUDT = info->isUDT;

	currentInvocation->function = self;

	/* Get the ClassLoader for the schema that this function belongs to
	 */
	loader = JNI_callStaticObjectMethod(s_Loader_class, s_Loader_getSchemaLoader, schemaName);
	JNI_deleteLocalRef(schemaName);

	elog(DEBUG1, "Loading class %s", info->className);
	className = String_createJavaStringFromNTS(info->className);

	tmp = JNI_callObjectMethod(loader, s_ClassLoader_loadClass, className);
	JNI_deleteLocalRef(loader);
	JNI_deleteLocalRef(className);

	self->clazz = (jclass)JNI_newGlobalRef(tmp);
	JNI_deleteLocalRef(tmp);

	if(self->isUDT)
	{
		setupUDT(self, info, procStruct);
		return;
	}

	if(CALLED_AS_TRIGGER(fcinfo))
	{
		self->func.nonudt.typeMap = 0;
		setupTriggerParams(self, info);
	}
	else
	{
		setupFunctionParams(self, info, procStruct, fcinfo);
	}


	initStringInfo(&sign);
	buildSignature(self, &sign, self->func.nonudt.returnType, false);

	elog(DEBUG1, "Obtaining method %s.%s %s", info->className, info->methodName, sign.data);
	self->func.nonudt.method = JNI_getStaticMethodIDOrNull(self->clazz, info->methodName, sign.data);

	if(self->func.nonudt.method == 0)
	{
		char* origSign = sign.data;
		Type altType = 0;
		Type realRetType = self->func.nonudt.returnType;

		elog(DEBUG1, "Method %s.%s %s not found", info->className, info->methodName, origSign);

		if(Type_isPrimitive(self->func.nonudt.returnType))
		{
			/*
			 * One valid reason for not finding the method is when
			 * the return type used in the signature is a primitive and
			 * the true return type of the method is the object class that
			 * corresponds to that primitive.
			 */
			altType = Type_getObjectType(self->func.nonudt.returnType);
			realRetType = altType;
		}
		else if(strcmp(Type_getJavaTypeName(self->func.nonudt.returnType), "java.sql.ResultSet") == 0)
		{
			/*
			 * Another reason might be that we expected a ResultSetProvider
			 * but the implementation returns a ResultSetHandle that needs to be
			 * wrapped. The wrapping is internal so we retain the original
			 * return type anyway.
			 */
			altType = realRetType;
		}

		if(altType != 0)
		{
			JNI_exceptionClear();
			initStringInfo(&sign);
			buildSignature(self, &sign, altType, true);

			elog(DEBUG1, "Obtaining method %s.%s %s", info->className, info->methodName, sign.data);
			self->func.nonudt.method = JNI_getStaticMethodIDOrNull(self->clazz, info->methodName, sign.data);
	
			if(self->func.nonudt.method != 0)
				self->func.nonudt.returnType = realRetType;
		}
		if(self->func.nonudt.method == 0)
			PgObject_throwMemberError(self->clazz, info->methodName, origSign, true, true);

		if(sign.data != origSign)
			pfree(origSign);
	}
	pfree(sign.data);
}

static Function Function_create(PG_FUNCTION_ARGS)
{
	ParseResultData info;
	Function self = (Function)PgObjectClass_allocInstance(s_FunctionClass, TopMemoryContext);
	HeapTuple procTup = PgObject_getValidTuple(PROCOID, fcinfo->flinfo->fn_oid, "function");

	memset(&info, 0, sizeof(ParseResultData));
	parseFunction(&info, procTup);
	Function_init(self, &info, (Form_pg_proc)GETSTRUCT(procTup), fcinfo);

	pfree(info.buffer);
	ReleaseSysCache(procTup);
	return self;
}

Function Function_getFunction(PG_FUNCTION_ARGS)
{
	Oid funcOid = fcinfo->flinfo->fn_oid;
	Function func = (Function)HashMap_getByOid(s_funcMap, funcOid);
	if(func == 0)
	{
		func = Function_create(fcinfo);
		HashMap_putByOid(s_funcMap, funcOid, func);
	}
	return func;
}

jobject Function_getTypeMap(Function self)
{
	return self->func.nonudt.typeMap;
}

static bool Function_inUse(Function func)
{
	Invocation* ic = currentInvocation;
	while(ic != 0)
	{
		if(ic->function == func)
			return true;
		ic = ic->previous;
	}
	return false;
}

void Function_clearFunctionCache(void)
{
	Entry entry;

	HashMap oldMap = s_funcMap;
	Iterator itor = Iterator_create(oldMap);

	s_funcMap = HashMap_create(59, TopMemoryContext);
	while((entry = Iterator_next(itor)) != 0)
	{
		Function func = (Function)Entry_getValue(entry);
		if(func != 0)
		{
			if(Function_inUse(func))
			{
				/* This is the replace_jar function or similar. Just
				 * move it to the new map.
				 */
				HashMap_put(s_funcMap, Entry_getKey(entry), func);
			}
			else
			{
				Entry_setValue(entry, 0);
				PgObject_free((PgObject)func);
			}
		}
	}
	PgObject_free((PgObject)itor);
	PgObject_free((PgObject)oldMap);
}

Datum Function_invoke(Function self, PG_FUNCTION_ARGS)
{
	Datum retVal;
	int32 top;
	jvalue* args;
	Type  invokerType;

	fcinfo->isnull = false;
	currentInvocation->function = self;

	if(self->isUDT)
		return self->func.udt.udtFunction(self->func.udt.udt, fcinfo);

	if(self->func.nonudt.isMultiCall && SRF_IS_FIRSTCALL())
		Invocation_assertDisconnect();

	top = self->func.nonudt.numParams;
	
	/* Leave room for one extra parameter. Functions that returns unmapped
	 * composite types must have a single row ResultSet as an OUT parameter.
	 */
	args  = (jvalue*)palloc((top + 1) * sizeof(jvalue));
	invokerType = self->func.nonudt.returnType;

	if(top > 0)
	{
		int32 idx;
		Type* types = self->func.nonudt.paramTypes;

		/* a class loader or other mechanism might have connected already. This
		 * connection must be dropped since its parent context is wrong.
		 */
		if(Type_isDynamic(invokerType))
			invokerType = Type_getRealType(invokerType, get_fn_expr_rettype(fcinfo->flinfo), self->func.nonudt.typeMap);

		for(idx = 0; idx < top; ++idx)
		{
			if(PG_ARGISNULL(idx))
				/*
				 * Set this argument to zero (or null in case of object)
				 */
				args[idx].j = 0L;
			else
			{
				Type paramType = types[idx];
				if(Type_isDynamic(paramType))
					paramType = Type_getRealType(paramType, get_fn_expr_argtype(fcinfo->flinfo, idx), self->func.nonudt.typeMap);
				args[idx] = Type_coerceDatum(paramType, PG_GETARG_DATUM(idx));
			}
		}
	}

	retVal = self->func.nonudt.isMultiCall
		? Type_invokeSRF(invokerType, self->clazz, self->func.nonudt.method, args, fcinfo)
		: Type_invoke(invokerType, self->clazz, self->func.nonudt.method, args, fcinfo);

	pfree(args);
	return retVal;
}

Datum Function_invokeTrigger(Function self, PG_FUNCTION_ARGS)
{
	jvalue arg;
	Datum  ret;

	arg.l = TriggerData_create((TriggerData*)fcinfo->context);
	if(arg.l == 0)
		return 0;

	currentInvocation->function = self;
	Type_invoke(self->func.nonudt.returnType, self->clazz, self->func.nonudt.method, &arg, fcinfo);

	fcinfo->isnull = false;
	if(JNI_exceptionCheck())
		ret = 0;
	else
	{
		/* A new Tuple may or may not be created here. If it is, ensure that
		 * it is created in the upper SPI context.
		 */
		MemoryContext currCtx = Invocation_switchToUpperContext();
		ret = PointerGetDatum(TriggerData_getTriggerReturnTuple(arg.l, &fcinfo->isnull));

		/* Triggers are not allowed to set the fcinfo->isnull, even when
		 * they return null.
		 */
		fcinfo->isnull = false;

		MemoryContextSwitchTo(currCtx);
	}

	JNI_deleteLocalRef(arg.l);
	return ret;
}

bool Function_isCurrentReadOnly(void)
{
	/* function will be 0 during resolve of class and java function. At
	 * that time, no updates are allowed (or needed).
	 */
	return (currentInvocation->function == 0)
		? true
		: currentInvocation->function->readOnly;
}

