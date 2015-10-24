/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>
#include <funcapi.h>
#include <utils/typcache.h>

#include "pljava/type/Type_priv.h"
#include "pljava/type/Composite.h"
#include "pljava/type/TupleDesc.h"
#include "pljava/type/HeapTupleHeader.h"
#include "pljava/Invocation.h"
#include "org_postgresql_pljava_jdbc_SingleRowReader.h"

struct Composite_
{
	/*
	 * The String "class" extends Type so the first
	 * entry must be the Type_ structure. This enables us
	 * to cast the CompositeType to a Type.
	 */
	struct Type_ Type_extension;

	/*
	 * The TupleDesc associated with the SETOF function.
	 */
	TupleDesc m_tupleDesc;
};

typedef struct Composite_* Composite;

static jclass s_ResultSetProvider_class;
static jmethodID s_ResultSetProvider_assignRowValues;
static jmethodID s_ResultSetProvider_close;

static jclass s_ResultSetHandle_class;
static jclass s_ResultSetPicker_class;
static jmethodID s_ResultSetPicker_init;

static jclass s_SingleRowReader_class;
static jmethodID s_SingleRowReader_init;

static jclass s_SingleRowWriter_class;
static jmethodID s_SingleRowWriter_init;
static jmethodID s_SingleRowWriter_getTupleAndClear;

static TypeClass s_CompositeClass;

static jobject _createWriter(jobject tupleDesc)
{
	return JNI_newObject(s_SingleRowWriter_class, s_SingleRowWriter_init, tupleDesc);
}

static HeapTuple _getTupleAndClear(jobject jrps)
{
	Ptr2Long p2l;

	if(jrps == 0)
		return 0;

	p2l.longVal = JNI_callLongMethod(jrps, s_SingleRowWriter_getTupleAndClear);
	return (HeapTuple)p2l.ptrVal;
}

/*
 * This function is a bit special in that it adds an additional parameter
 * to the parameter list (a java.sql.ResultSet implemented as a
 * SingleRowWriter) and calls a boolean method. It's assumed that the
 * SingleRowWriter has been initialized with values if the method returns
 * true. If so, the values are obtained in the form of a HeapTuple which in
 * turn is returned (as a Datum) from this method.
 */
static Datum _Composite_invoke(Type self, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS)
{
	bool hasRow;
	Datum result = 0;
	TupleDesc tupleDesc = Type_getTupleDesc(self, fcinfo);
	jobject jtd = TupleDesc_create(tupleDesc);
	jobject singleRowWriter = _createWriter(jtd);
	int numArgs = fcinfo->nargs;
	
	// Caller guarantees room for one extra slot
	//
	args[numArgs].l = singleRowWriter;
	hasRow = (JNI_callStaticBooleanMethodA(cls, method, args) == JNI_TRUE);

	if(hasRow)
	{
		/* Obtain tuple and return it as a Datum. Must be done using a more
		 * durable context.
		 */
		MemoryContext currCtx = Invocation_switchToUpperContext();
		HeapTuple tuple = _getTupleAndClear(singleRowWriter);
	    result = HeapTupleGetDatum(tuple);
		MemoryContextSwitchTo(currCtx);
	}
	else
		fcinfo->isnull = true;

	JNI_deleteLocalRef(jtd);
	JNI_deleteLocalRef(singleRowWriter);
	return result;
}

static jobject _Composite_getSRFProducer(Type self, jclass cls, jmethodID method, jvalue* args)
{
	jobject tmp = JNI_callStaticObjectMethodA(cls, method, args);
	if(tmp != 0 && JNI_isInstanceOf(tmp, s_ResultSetHandle_class))
	{
		jobject wrapper = JNI_newObject(s_ResultSetPicker_class, s_ResultSetPicker_init, tmp);
		JNI_deleteLocalRef(tmp);
		tmp = wrapper;
	}
	return tmp;
}

static jobject _Composite_getSRFCollector(Type self, PG_FUNCTION_ARGS)
{
	jobject tmp1;
	jobject tmp2;
	TupleDesc tupleDesc = Type_getTupleDesc(self, fcinfo);
	if(tupleDesc == 0)
		ereport(ERROR, (errmsg("Unable to find tuple descriptor")));

	tmp1 = TupleDesc_create(tupleDesc);
	tmp2 = _createWriter(tmp1);
	JNI_deleteLocalRef(tmp1);
	return tmp2;
}

static bool _Composite_hasNextSRF(Type self, jobject rowProducer, jobject rowCollector, jint callCounter)
{
	/* Obtain next row using the RowCollector as a parameter to the
	 * ResultSetProvider.assignRowValues method.
	 */
	return (JNI_callBooleanMethod(rowProducer,
			s_ResultSetProvider_assignRowValues,
			rowCollector,
			callCounter) == JNI_TRUE);
}

static Datum _Composite_nextSRF(Type self, jobject rowProducer, jobject rowCollector)
{
	Datum result = 0;
	HeapTuple tuple = _getTupleAndClear(rowCollector);
	if(tuple != 0)
		result = HeapTupleGetDatum(tuple);
	return result;
}

static void _Composite_closeSRF(Type self, jobject rowProducer)
{
	JNI_callVoidMethod(rowProducer, s_ResultSetProvider_close);
}

/* Assume that the Datum is a HeapTupleHeader and convert it into
 * a SingleRowReader instance.
 */
static jvalue _Composite_coerceDatum(Type self, Datum arg)
{
	jobject tupleDesc;
	jvalue result;
	jlong pointer;
	HeapTupleHeader hth = DatumGetHeapTupleHeader(arg);

	result.l = 0;
	if(hth == 0)
		return result;

	tupleDesc = HeapTupleHeader_getTupleDesc(hth);
	pointer = Invocation_createLocalWrapper(hth);
	result.l = JNI_newObject(s_SingleRowReader_class, s_SingleRowReader_init, pointer, tupleDesc);
	JNI_deleteLocalRef(tupleDesc);
	return result;
}

static TupleDesc createGlobalTupleDescCopy(TupleDesc td)
{
	MemoryContext curr = MemoryContextSwitchTo(TopMemoryContext);
	td = CreateTupleDescCopyConstr(td);
	MemoryContextSwitchTo(curr);
	return td;
}

static TupleDesc _Composite_getTupleDesc(Type self, PG_FUNCTION_ARGS)
{
	TupleDesc td = ((Composite)self)->m_tupleDesc;
	if(td != 0)
		return td;

	switch(get_call_result_type(fcinfo, 0, &td))
	{
		case TYPEFUNC_COMPOSITE:
			if(td->tdtypeid == RECORDOID)
				/*
				 * We can't hold on to this one. It's anonymous
				 * and may vary between calls.
				 */
				td = CreateTupleDescCopy(td);
			else
			{
				td = createGlobalTupleDescCopy(td);
				((Composite)self)->m_tupleDesc = td;
			}
			break;
		default:
			ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));
	}
	return td;
}

static const char* _Composite_getJNIReturnSignature(Type self, bool forMultiCall, bool useAltRepr)
{
	return forMultiCall
		? (useAltRepr
			? "Lorg/postgresql/pljava/ResultSetHandle;"
			: "Lorg/postgresql/pljava/ResultSetProvider;")
		: "Z";
}

Type Composite_obtain(Oid typeId)
{
	Composite infant = (Composite)TypeClass_allocInstance(s_CompositeClass, typeId);
	if(typeId == RECORDOID)
		infant->m_tupleDesc = 0;
	else
	{
		TupleDesc tmp = lookup_rowtype_tupdesc(typeId, -1);
		infant->m_tupleDesc = createGlobalTupleDescCopy(tmp);
		ReleaseTupleDesc(tmp);
	}
	return (Type)infant;
}

/* Make this datatype available to the postgres system.
 */
extern void Composite_initialize(void);
void Composite_initialize(void)
{
	JNINativeMethod methods[] =
	{
		{
		"_getObject",
	  	"(JJI)Ljava/lang/Object;",
	  	Java_org_postgresql_pljava_jdbc_SingleRowReader__1getObject
		},
		{
		"_free",
		"(J)V",
		Java_org_postgresql_pljava_jdbc_SingleRowReader__1free
		},
		{ 0, 0, 0 }
	};

	s_SingleRowReader_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/jdbc/SingleRowReader"));
	PgObject_registerNatives2(s_SingleRowReader_class, methods);
	s_SingleRowReader_init = PgObject_getJavaMethod(s_SingleRowReader_class, "<init>", "(JLorg/postgresql/pljava/internal/TupleDesc;)V");

	s_SingleRowWriter_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/jdbc/SingleRowWriter"));
	s_SingleRowWriter_init = PgObject_getJavaMethod(s_SingleRowWriter_class, "<init>", "(Lorg/postgresql/pljava/internal/TupleDesc;)V");
	s_SingleRowWriter_getTupleAndClear = PgObject_getJavaMethod(s_SingleRowWriter_class, "getTupleAndClear", "()J");

	s_ResultSetProvider_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/ResultSetProvider"));
	s_ResultSetProvider_assignRowValues = PgObject_getJavaMethod(s_ResultSetProvider_class, "assignRowValues", "(Ljava/sql/ResultSet;I)Z");
	s_ResultSetProvider_close = PgObject_getJavaMethod(s_ResultSetProvider_class, "close", "()V");

	s_ResultSetHandle_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/ResultSetHandle"));
	s_ResultSetPicker_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/ResultSetPicker"));
	s_ResultSetPicker_init = PgObject_getJavaMethod(s_ResultSetPicker_class, "<init>", "(Lorg/postgresql/pljava/ResultSetHandle;)V");

	s_CompositeClass = TypeClass_alloc2("type.Composite", sizeof(struct TypeClass_), sizeof(struct Composite_));
	s_CompositeClass->JNISignature    = "Ljava/sql/ResultSet;";
	s_CompositeClass->javaTypeName    = "java.sql.ResultSet";
	s_CompositeClass->getTupleDesc    = _Composite_getTupleDesc;
	s_CompositeClass->coerceDatum     = _Composite_coerceDatum;
	s_CompositeClass->invoke          = _Composite_invoke;
	s_CompositeClass->getSRFProducer  = _Composite_getSRFProducer;
	s_CompositeClass->getSRFCollector = _Composite_getSRFCollector;
	s_CompositeClass->hasNextSRF      = _Composite_hasNextSRF;
	s_CompositeClass->nextSRF         = _Composite_nextSRF;
	s_CompositeClass->closeSRF        = _Composite_closeSRF;
	s_CompositeClass->getJNIReturnSignature = _Composite_getJNIReturnSignature;
	s_CompositeClass->outParameter    = true;

	Type_registerType2(InvalidOid, "java.sql.ResultSet", Composite_obtain);
}

/****************************************
 * JNI methods
 ****************************************/

/*
 * Class:     org_postgresql_pljava_jdbc_SingleRowReader
 * Method:    _free
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_jdbc_SingleRowReader__1free(JNIEnv* env, jobject _this, jlong hth)
{
	HeapTupleHeader_free(env, hth);
}

/*
 * Class:     org_postgresql_pljava_jdbc_SingleRowReader
 * Method:    _getObject
 * Signature: (JJI)Ljava/lang/Object;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_jdbc_SingleRowReader__1getObject(JNIEnv* env, jclass clazz, jlong hth, jlong jtd, jint attrNo)
{
	return HeapTupleHeader_getObject(env, hth, jtd, attrNo);
}
