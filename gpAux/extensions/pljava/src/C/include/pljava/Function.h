/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_Function_h
#define __pljava_Function_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <commands/trigger.h>

/*******************************************************************
 * The Function instance is the most central class of the Pl/Java
 * system. Parsing of the "AS" (later to become "EXTERNAL NAME")
 * and class/method lookups is done here.
 * 
 * A Function instance knows how to coerce all Datum parameters into
 * their Java equivalent, how to call its assocated Java class and
 * method and how to coerce the Java return value into a Datum.
 * 
 * Functions are cached using their Oid. They live in TopMemoryContext.
 * 
 * @author Thomas Hallgren
 *
 *******************************************************************/

/*
 * Clear all cached function to method entries. This is called after a
 * successful replace_jar operation.
 */
extern void Function_clearFunctionCache(void);

/*
 * Get a Function using a function Oid. If the function is not found, one
 * will be created based on the class and method name denoted in the "AS"
 * clause, the parameter types, and the return value of the function
 * description. If "isTrigger" is set to true, the parameter type and
 * return value of the function will be fixed to:
 * 
 * org.postgresql.pljava.Tuple <method name>(org.postgresql.pljava.TriggerData td)
 */
extern Function Function_getFunction(PG_FUNCTION_ARGS);

/*
 * Invoke a function, i.e. coerce the parameters, call the java method, and
 * coerce the return value back to a Datum.
 */
extern Datum Function_invoke(Function self, PG_FUNCTION_ARGS);

/*
 * Invoke a trigger. Wrap the TriggerData in org.postgresql.pljava.TriggerData
 * object, make the call, and unwrap the resulting Tuple.
 */
extern Datum Function_invokeTrigger(Function self, PG_FUNCTION_ARGS);

/*
 * Returns the Type Map that is associated with the function
 */
extern jobject Function_getTypeMap(Function self);

/*
 * Returns true if the currently executing function is non volatile, i.e. stable
 * or immutable. Such functions are not allowed to have side effects.
 */
extern bool Function_isCurrentReadOnly(void);

#ifdef __cplusplus
}
#endif
#endif
