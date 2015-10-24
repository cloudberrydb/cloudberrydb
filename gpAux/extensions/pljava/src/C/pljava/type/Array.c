/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include "pljava/type/Type_priv.h"
#include "pljava/type/Array.h"
#include "pljava/Invocation.h"

void arraySetNull(bits8* bitmap, int offset, bool flag)
{
	if(bitmap != 0)
	{
		int bitmask = 1 << (offset % 8);	
		bitmap += offset / 8;
		if(flag)
			*bitmap &= ~bitmask;
		else
			*bitmap |= bitmask;
	}
}

bool arrayIsNull(const bits8* bitmap, int offset)
{
	return bitmap == 0 ? false : !(bitmap[offset / 8] & (1 << (offset % 8)));
}

ArrayType* createArrayType(jsize nElems, size_t elemSize, Oid elemType, bool withNulls)
{
	ArrayType* v;
	int nBytes = elemSize * nElems;
	MemoryContext currCtx = Invocation_switchToUpperContext();
	int dataoffset;
	if(withNulls)
	{
		dataoffset = ARR_OVERHEAD_WITHNULLS(1, nElems);
		nBytes += dataoffset;
	}
	else
	{
		dataoffset = 0;			/* marker for no null bitmap */
		nBytes += ARR_OVERHEAD_NONULLS(1);
	}
	v = (ArrayType*)palloc0(nBytes);
	v->dataoffset = dataoffset;
	MemoryContextSwitchTo(currCtx);

	SET_VARSIZE(v, nBytes);
	ARR_NDIM(v) = 1;
	ARR_ELEMTYPE(v) = elemType;
	*((int*)ARR_DIMS(v)) = nElems;
	*((int*)ARR_LBOUND(v)) = 1;
	return v;
}

static jvalue _Array_coerceDatum(Type self, Datum arg)
{
	jvalue result;
	jsize idx;
	Type  elemType    = Type_getElementType(self);
	int16 elemLength  = Type_getLength(elemType);
	char  elemAlign   = Type_getAlign(elemType);
	bool  elemByValue = Type_isByValue(elemType);
	ArrayType* v = DatumGetArrayTypeP(arg);
	jsize nElems = (jsize)ArrayGetNItems(ARR_NDIM(v), ARR_DIMS(v));
	jobjectArray objArray = JNI_newObjectArray(nElems, Type_getJavaClass(elemType), 0);
	char* values = ARR_DATA_PTR(v);
	bits8* nullBitMap = ARR_NULLBITMAP(v);

	for(idx = 0; idx < nElems; ++idx)
	{
		if(arrayIsNull(nullBitMap, idx))
			JNI_setObjectArrayElement(objArray, idx, 0);
		else
		{
			Datum value = fetch_att(values, elemByValue, elemLength);
			jvalue obj = Type_coerceDatum(elemType, value);
			JNI_setObjectArrayElement(objArray, idx, obj.l);
			JNI_deleteLocalRef(obj.l);

			values = att_addlength_datum(values, elemLength, PointerGetDatum(values));
			values = (char*)att_align_nominal(values, elemAlign);
		}
	}
	result.l = (jobject)objArray;
	return result;
}

static Datum _Array_coerceObject(Type self, jobject objArray)
{
	ArrayType* v;
	jsize idx;
	int    lowerBound = 1;
	Type   elemType = Type_getElementType(self);
	int    nElems   = (int)JNI_getArrayLength((jarray)objArray);
	Datum* values   = (Datum*)palloc(nElems * sizeof(Datum) + nElems * sizeof(bool));
	bool*  nulls    = (bool*)(values + nElems);

	for(idx = 0; idx < nElems; ++idx)
	{
		jobject obj = JNI_getObjectArrayElement(objArray, idx);
		if(obj == 0)
		{
			nulls[idx] = true;
			values[idx] = 0;
		}
		else
		{
			nulls[idx] = false;
			values[idx] = Type_coerceObject(elemType, obj);
			JNI_deleteLocalRef(obj);
		}
	}

	v = construct_md_array(
		values,
		nulls,
		1,
		&nElems,
		&lowerBound,
		Type_getOid(elemType),
		Type_getLength(elemType),
		Type_isByValue(elemType),
		Type_getAlign(elemType));

	pfree(values);
	PG_RETURN_ARRAYTYPE_P(v);
}

static bool _Array_canReplaceType(Type self, Type other)
{
	Type oe = Type_getElementType(other);
	return oe == 0 ? false : Type_canReplaceType(Type_getElementType(self), oe);
}

Type Array_fromOid(Oid typeId, Type elementType)
{
	return Array_fromOid2(typeId, elementType, _Array_coerceDatum, _Array_coerceObject);
}

Type Array_fromOid2(Oid typeId, Type elementType, DatumCoercer coerceDatum, ObjectCoercer coerceObject)
{
	Type self;
	TypeClass arrayClass;
	const char* elemClassName    = PgObjectClass_getName(PgObject_getClass((PgObject)elementType));
	const char* elemJNISignature = Type_getJNISignature(elementType);
	const char* elemJavaTypeName = Type_getJavaTypeName(elementType);

	MemoryContext currCtx = MemoryContextSwitchTo(TopMemoryContext);

	char* tmp = palloc(strlen(elemClassName) + 3);
	sprintf(tmp, "%s[]", elemClassName);
	arrayClass = TypeClass_alloc(tmp);

	tmp = palloc(strlen(elemJNISignature) + 2);
	sprintf(tmp, "[%s", elemJNISignature);
	arrayClass->JNISignature = tmp;

	tmp = palloc(strlen(elemJavaTypeName) + 3);
	sprintf(tmp, "%s[]", elemJavaTypeName);
	arrayClass->javaTypeName = tmp;
	arrayClass->coerceDatum  = coerceDatum;
	arrayClass->coerceObject = coerceObject;
	arrayClass->canReplaceType = _Array_canReplaceType;
	self = TypeClass_allocInstance(arrayClass, typeId);
	MemoryContextSwitchTo(currCtx);

	self->elementType = elementType;
	Type_registerType(arrayClass->javaTypeName, self);

	if(Type_isPrimitive(elementType))
		self->objectType = Array_fromOid(typeId, Type_getObjectType(elementType));
	return self;
}

