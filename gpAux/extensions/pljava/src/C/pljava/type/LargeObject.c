/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#include <postgres.h>

#include "org_postgresql_pljava_internal_LargeObject.h"
#include "pljava/Exception.h"
#include "pljava/Invocation.h"
#include "pljava/type/Type_priv.h"
#include "pljava/type/Oid.h"
#include "pljava/type/LargeObject.h"

static jclass    s_LargeObject_class;
static jmethodID s_LargeObject_init;

/*
 * org.postgresql.pljava.type.LargeObject type.
 */
jobject LargeObject_create(LargeObjectDesc* lo)
{
	jobject jlo;
	Ptr2Long loH;

	if(lo == 0)
		return 0;

	loH.longVal = 0L; /* ensure that the rest is zeroed out */
	loH.ptrVal = lo;
	jlo = JNI_newObject(s_LargeObject_class, s_LargeObject_init, loH.longVal);
	return jlo;
}

extern void LargeObject_initialize(void);
void LargeObject_initialize(void)
{
	TypeClass cls;
	JNINativeMethod methods[] =
	{
		{
		"_create",
	  	"(I)Lorg/postgresql/pljava/internal/Oid;",
	  	Java_org_postgresql_pljava_internal_LargeObject__1create
		},
		{
		"_drop",
	  	"(Lorg/postgresql/pljava/internal/Oid;)I",
	  	Java_org_postgresql_pljava_internal_LargeObject__1drop
		},
		{
		"_open",
	  	"(Lorg/postgresql/pljava/internal/Oid;I)Lorg/postgresql/pljava/internal/LargeObject;",
	  	Java_org_postgresql_pljava_internal_LargeObject__1open
		},
		{
		"_close",
	  	"(J)V",
	  	Java_org_postgresql_pljava_internal_LargeObject__1close
		},
		{
		"_getId",
	  	"(J)Lorg/postgresql/pljava/internal/Oid;",
	  	Java_org_postgresql_pljava_internal_LargeObject__1getId
		},
		{
		"_length",
	  	"(J)J",
	  	Java_org_postgresql_pljava_internal_LargeObject__1length
		},
		{
		"_seek",
	  	"(JJI)J",
	  	Java_org_postgresql_pljava_internal_LargeObject__1seek
		},
		{
		"_tell",
	  	"(J)J",
	  	Java_org_postgresql_pljava_internal_LargeObject__1tell
		},
		{
		"_read",
	  	"(J[B)I",
	  	Java_org_postgresql_pljava_internal_LargeObject__1read
		},
		{
		"_write",
	  	"(J[B)I",
	  	Java_org_postgresql_pljava_internal_LargeObject__1write
		},
		{ 0, 0, 0 }
	};

	s_LargeObject_class = JNI_newGlobalRef(PgObject_getJavaClass("org/postgresql/pljava/internal/LargeObject"));
	PgObject_registerNatives2(s_LargeObject_class, methods);
	s_LargeObject_init = PgObject_getJavaMethod(s_LargeObject_class, "<init>", "(J)V");

	cls = TypeClass_alloc("type.LargeObject");
	cls->JNISignature = "Lorg/postgresql/pljava/internal/LargeObject;";
	cls->javaTypeName = "org.postgresql.pljava.internal.LargeObject";
	Type_registerType("org.postgresql.pljava.internal.LargeObject", TypeClass_allocInstance(cls, InvalidOid));
}

/****************************************
 * JNI methods
 ****************************************/
/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _create
 * Signature: (I)Lorg/postgresql/pljava/internal/LargeObject;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1create(JNIEnv* env, jclass cls, jint flags)
{
	jobject result = 0;

	BEGIN_NATIVE
	PG_TRY();
	{
		result = Oid_create(inv_create((int)flags));
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("inv_create");
	}
	PG_END_TRY();
	END_NATIVE

	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _drop
 * Signature: (Lorg/postgresql/pljava/internal/Oid;)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1drop(JNIEnv* env, jclass cls, jobject oid)
{
	jint result = -1;
	BEGIN_NATIVE
	PG_TRY();
	{
		result = inv_drop(Oid_getOid(oid));
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("inv_drop");
	}
	PG_END_TRY();
	END_NATIVE
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _open
 * Signature: (Lorg/postgresql/pljava/internal/Oid;I)Lorg/postgresql/pljava/internal/LargeObject;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1open(JNIEnv* env, jclass cls, jobject oid, jint flags)
{
	jobject result = 0;
	BEGIN_NATIVE
	PG_TRY();
	{
		result = LargeObject_create(inv_open(Oid_getOid(oid), (int)flags, JavaMemoryContext));
	}
	PG_CATCH();
	{
		Exception_throw_ERROR("inv_open");
	}
	PG_END_TRY();
	END_NATIVE
	return result;
}


/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _close
 * Signature: (J)V
 */
JNIEXPORT void JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1close(JNIEnv* env, jclass cls, jlong _this)
{
	LargeObjectDesc* self = Invocation_getWrappedPointer(_this);
	if(self != 0)
	{
		BEGIN_NATIVE
		PG_TRY();
		{
			inv_close(self);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("inv_close");
		}
		PG_END_TRY();
		END_NATIVE
	}
}

/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _getId
 * Signature: (J)Lorg/postgresql/pljava/internal/Oid;
 */
JNIEXPORT jobject JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1getId(JNIEnv* env, jclass cls, jlong _this)
{
	jobject result = 0;
	LargeObjectDesc* self = Invocation_getWrappedPointer(_this);
	if(self != 0)
	{
		BEGIN_NATIVE
		result = Oid_create(self->id);
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _length
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1length(JNIEnv* env, jclass cls, jlong _this)
{
	jlong result = 0;
	LargeObjectDesc* self = Invocation_getWrappedPointer(_this);
	if(self != 0)
	{
		BEGIN_NATIVE
		PG_TRY();
		{
			/* There's no inv_length call so we use inv_seek on
			 * a temporary LargeObjectDesc.
			 */
			LargeObjectDesc lod;
			memcpy(&lod, self, sizeof(LargeObjectDesc));
			result = (jlong)inv_seek(&lod, 0, SEEK_END);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("inv_seek");
		}
		PG_END_TRY();
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _seek
 * Signature: (JJI)J
 */
JNIEXPORT jlong JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1seek(JNIEnv* env, jclass cls, jlong _this, jlong pos, jint whence)
{
	jlong result = 0;
	LargeObjectDesc* self = Invocation_getWrappedPointer(_this);
	if(self != 0)
	{
		BEGIN_NATIVE
		PG_TRY();
		{
			result = (jlong)inv_seek(self, (int)pos, (int)whence);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("inv_seek");
		}
		PG_END_TRY();
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _tell
 * Signature: (J)J
 */
JNIEXPORT jlong JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1tell(JNIEnv* env, jclass cls, jlong _this)
{
	jlong result = 0;
	LargeObjectDesc* self = Invocation_getWrappedPointer(_this);
	if(self != 0)
	{
		BEGIN_NATIVE
		PG_TRY();
		{
			result = (jlong)inv_tell(self);
		}
		PG_CATCH();
		{
			Exception_throw_ERROR("inv_tell");
		}
		PG_END_TRY();
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _read
 * Signature: (J[B)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1read(JNIEnv* env, jclass cls, jlong _this, jbyteArray buf)
{
	jint result = -1;
	LargeObjectDesc* self = Invocation_getWrappedPointer(_this);

	if(self != 0 && buf != 0)
	{
		BEGIN_NATIVE
		jint nBytes = JNI_getArrayLength(buf);
		if(nBytes != 0)
		{
			jbyte* byteBuf = JNI_getByteArrayElements(buf, 0);
			if(byteBuf != 0)
			{
				PG_TRY();
				{
					result = (jint)inv_read(self, (char*)byteBuf, (int)nBytes);
					JNI_releaseByteArrayElements(buf, byteBuf, 0);
				}
				PG_CATCH();
				{
					JNI_releaseByteArrayElements(buf, byteBuf, JNI_ABORT);
					Exception_throw_ERROR("inv_read");
				}
				PG_END_TRY();
			}
		}
		END_NATIVE
	}
	return result;
}

/*
 * Class:     org_postgresql_pljava_internal_LargeObject
 * Method:    _write
 * Signature: (J[B)I
 */
JNIEXPORT jint JNICALL
Java_org_postgresql_pljava_internal_LargeObject__1write(JNIEnv* env, jclass cls, jlong _this, jbyteArray buf)
{
	jint result = -1;
	LargeObjectDesc* self = Invocation_getWrappedPointer(_this);

	if(self != 0 && buf != 0)
	{
		BEGIN_NATIVE
		jint nBytes = JNI_getArrayLength(buf);
		if(nBytes != 0)
		{
			jbyte* byteBuf = JNI_getByteArrayElements(buf, 0);
			if(byteBuf != 0)
			{
				PG_TRY();
				{
					result = (jint)inv_write(self, (char*)byteBuf, nBytes);
					
					/* No need to copy bytes back, hence the JNI_ABORT */
					JNI_releaseByteArrayElements(buf, byteBuf, JNI_ABORT);
				}
				PG_CATCH();
				{
					JNI_releaseByteArrayElements(buf, byteBuf, JNI_ABORT);
					Exception_throw_ERROR("inv_write");
				}
				PG_END_TRY();
			}
		}
		END_NATIVE
	}
	return result;
}
