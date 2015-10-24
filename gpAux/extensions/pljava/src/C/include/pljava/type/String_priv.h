/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_type_String_priv_h
#define __pljava_type_String_priv_h

#include "pljava/type/Type_priv.h"
#include "pljava/type/String.h"

#ifdef __cplusplus
extern "C" {
#endif

/**************************************************************************
 * @author Thomas Hallgren
 **************************************************************************/

struct String_
{
	/*
	 * The String "class" extends Type so the first
	 * entry must be the Type_ structure. This enables us
	 * to cast the String to a Type.
	 */
	struct Type_ Type_extension;

	/*
	 * Transforms text into a Datum.
	 */
	FmgrInfo textInput;
	
	/*
	 * Transforms datum into text.
	 */
	FmgrInfo textOutput;

	/*
	 * Oid of elment type (if any)
	 */
	Oid elementType;
};

extern Datum _String_coerceObject(Type self, jobject jstr);

extern jvalue _String_coerceDatum(Type self, Datum value);

#ifdef __cplusplus
}
#endif
#endif
