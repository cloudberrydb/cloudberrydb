/*-------------------------------------------------------------------------
 *
 * printtup_vec.h
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/include/access/printtup_vec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PRINTTUP_VEC_H
#define PRINTTUP_VEC_H

#include "postgres.h"
#include "tcop/dest.h"
#include "utils/tuptable_vec.h"

typedef bool (*printtup_func)(TupleTableSlot *slot,
									 DestReceiver *self);
typedef void (*rStartup_func) (DestReceiver *self,
							 int operation,
							 TupleDesc typeinfo);
extern bool printtup_wrapper(TupleTableSlot *slot, DestReceiver *self);
extern void set_printtup_wrapper(DestReceiver** dest);
typedef bool (*receiveSlot) (TupleTableSlot *slot,
                                                                DestReceiver *self);
extern bool vec_slot_loop(TupleTableSlot *slot, DestReceiver *self, receiveSlot callback);
extern printtup_func printtup_prev;

#endif   /* PRINTTUP_VEC_H */
