/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 *
 * @author Thomas Hallgren
 */
#ifndef __pljava_TriggerData_h
#define __pljava_TriggerData_h

#include "pljava/type/Type.h"

#ifdef __cplusplus
extern "C" {
#endif

#include <commands/trigger.h>

/**********************************************************************
 * The TriggerData java class extends the NativeStruct and provides JNI
 * access to some of the attributes of the TriggerData structure.
 * 
 * @author Thomas Hallgren
 **********************************************************************/
 
/*
 * Create the org.postgresql.pljava.TriggerData object.
 */
extern jobject TriggerData_create(TriggerData* triggerData);

/*
 * Obtains the returned Tuple after trigger has been processed.
 */
extern HeapTuple TriggerData_getTriggerReturnTuple(jobject jtd, bool* wasNull);

#ifdef __cplusplus
}
#endif
#endif
