/*--------------------------------------------------------------------
 * wrapper.h
 *	  Wrap the underlying arrow library

 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/utils/wrapper.h
 *
 *--------------------------------------------------------------------
 */
#ifndef BASIC_H
#define BASIC_H

#include "utils/arrow.h"

extern void free_array(void **ptr_array);
extern void free_batch(void **ptr_batch);
extern void free_batch(void **ptr_batch);
extern void free_array_buffer(void **buffer);
extern void free_glib_obj_misc(void **obj);
extern void free_prj_project(void **project);
extern void free_gandiva_node(void **gn);

extern bool is_type_unsupported_by_vector_array(Oid typeOid, int nodeType);
extern void free_glib_obj_misc(void **obj);
void *get_and_reset_auto_ptr(void **ptr);
extern void free_array_builder(void **builder);
extern void free_scalar_datum(void **scalar_datum);
extern void free_array_datum(void **array_datum);

#endif   /* wrapper */
