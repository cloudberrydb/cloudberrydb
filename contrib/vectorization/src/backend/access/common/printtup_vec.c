/*-------------------------------------------------------------------------
 *
 * printtup_vec.c
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  src/backend/access/common/printtup_vec.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/printtup.h"
#include "access/printtup_vec.h"
#include "executor/tstoreReceiver.h"
#include "vecexecutor/execslot.h"
#include "hook/hook.h"

printtup_func printtup_prev = NULL;
rStartup_func  tstoreStartupReceiver_prev = NULL;

bool  vec_slot_loop(TupleTableSlot *slot, DestReceiver *self, receiveSlot callback)
{
	TupleTableSlot * single_slot = MakeSingleTupleTableSlot(slot->tts_tupleDescriptor, &TTSOpsVirtual);
	ItemPointer itid = (ItemPointer)palloc0(sizeof(ItemPointerData));

	for (int i = 0; i < GetNumRows(slot); i++)
	{
		single_slot = ExecGetVirtualSlotFromVec(slot, i, single_slot, itid);
		callback(single_slot, self);
	}

	pfree(itid);
	ExecDropSingleTupleTableSlot(single_slot);

	ExecClearTuple(slot);

	return true;
}

static bool
vec_spi_printtup(TupleTableSlot *slot, DestReceiver *self)
{
	bool status;

	if (TTS_IS_VECTOR(slot))
	{
		status = vec_slot_loop(slot, self, printtup_prev);
	}
	else
	{
		status = printtup_prev(slot, self);
	}

	return status;
}

static void
vec_spi_dest_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	spi_dest_startup(self, operation, typeinfo);
}

static void
vec_donothingCleanup(DestReceiver *self)
{
	/* this is used for both shutdown and destroy methods */
}

static DestReceiver vec_spi_printtupDR = {
	vec_spi_printtup, vec_spi_dest_startup, vec_donothingCleanup, vec_donothingCleanup,
	DestSPI
};

bool
printtup_wrapper(TupleTableSlot *slot, DestReceiver *self)
{
	if (TTS_IS_VECTOR(slot))
		vec_slot_loop(slot, self, printtup_prev);
	else
		printtup_prev(slot, self);

	return true;
}

static void VectstoreStartupReceiver(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	tstoreStartupReceiver_prev(self, operation, typeinfo);
	printtup_prev = self->receiveSlot;
	self->receiveSlot = printtup_wrapper;
}

void
set_printtup_wrapper(DestReceiver** dest)
{
	if ((*dest)->mydest == DestSPI)
	{
		printtup_prev = (*dest)->receiveSlot;
		(*dest) = (DestReceiver *)&vec_spi_printtupDR;
		return ;
	}

	if ((*dest)->mydest == DestTuplestore)
	{
		tstoreStartupReceiver_prev = (*dest)->rStartup;
		(*dest)->rStartup = VectstoreStartupReceiver;
		return ;
	}

	if ((*dest)->receiveSlot != printtup_wrapper && (*dest)->receiveSlot != donothingVecReceive)
	{
		printtup_prev = (*dest)->receiveSlot;
		(*dest)->receiveSlot = printtup_wrapper;
	}
}
