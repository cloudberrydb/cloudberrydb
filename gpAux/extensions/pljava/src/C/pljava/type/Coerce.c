/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "pljava/type/Type_priv.h"
#include "pljava/type/Coerce.h"
#include "pljava/HashMap.h"
#include "pljava/Invocation.h"

static TypeClass s_coerceInClass;
static TypeClass s_coerceOutClass;

struct Coerce_
{
	/*
	 * The UDT "class" extends Type so the first
	 * entry must be the Type_ structure. This enables us
	 * to cast the String to a Type.
	 */
	struct Type_ Type_extension;

	Type innerType;

	Type outerType;

	FmgrInfo coerceFunction;
};

typedef struct Coerce_* Coerce;

static Datum _Coerce_invoke(Type type, jclass cls, jmethodID method, jvalue* args, PG_FUNCTION_ARGS)
{
	Coerce self = (Coerce)type;
	Datum arg = Type_invoke(self->innerType, cls, method, args, fcinfo);
	if(arg != 0)
	{
		MemoryContext currCtx = Invocation_switchToUpperContext();
		arg = FunctionCall1(&self->coerceFunction, arg);
		MemoryContextSwitchTo(currCtx);
	}
	return arg;
}

static jvalue _Coerce_coerceDatum(Type type, Datum arg)
{
	jvalue result;
	Coerce self = (Coerce)type;
	if(arg == 0)
		result.j = 0;
	else
		result = Type_coerceDatum(self->innerType, FunctionCall1(&self->coerceFunction, arg));
	return result;
}

static Datum _Coerce_coerceObject(Type type, jobject jval)
{
	Coerce self = (Coerce)type;
	Datum arg = Type_coerceObject(self->innerType, jval);
	if(arg != 0)
	{
		MemoryContext currCtx = Invocation_switchToUpperContext();
		arg = FunctionCall1(&self->coerceFunction, arg);
		MemoryContextSwitchTo(currCtx);
	}
	return arg;
}

static const char* _Coerce_getJNISignature(Type self)
{
	return Type_getJNISignature(((Coerce)self)->innerType);
}

static Type _Coerce_create(TypeClass coerceClass, Type innerType, Type outerType, Oid coerceFunctionID)
{
	Coerce self = (Coerce)TypeClass_allocInstance(coerceClass, Type_getOid(outerType));
	fmgr_info_cxt(coerceFunctionID, &self->coerceFunction, GetMemoryChunkContext(self));
	self->innerType = innerType;
	self->outerType = outerType;
	if(Type_isPrimitive(self->innerType))
		((Type)self)->objectType = _Coerce_create(coerceClass, Type_getObjectType(self->innerType), outerType, coerceFunctionID);
	return (Type)self;
}

Type Coerce_createIn(Type innerType, Type outerType, Oid coerceFunctionID)
{
	return _Coerce_create(s_coerceInClass, innerType, outerType, coerceFunctionID);
}

Type Coerce_createOut(Type innerType, Type outerType, Oid coerceFunctionID)
{
	return _Coerce_create(s_coerceOutClass, innerType, outerType, coerceFunctionID);
}

extern void Coerce_initialize(void);
void Coerce_initialize(void)
{
	s_coerceInClass = TypeClass_alloc2("type.CoerceIn", sizeof(struct TypeClass_), sizeof(struct Coerce_));
	s_coerceInClass->getJNISignature = _Coerce_getJNISignature;
	s_coerceInClass->coerceDatum = _Coerce_coerceDatum;

	s_coerceOutClass = TypeClass_alloc2("type.CoerceOut", sizeof(struct TypeClass_), sizeof(struct Coerce_));
	s_coerceOutClass->getJNISignature = _Coerce_getJNISignature;
	s_coerceOutClass->invoke = _Coerce_invoke;
	s_coerceOutClass->coerceObject = _Coerce_coerceObject;
}
