//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		IMDTrigger.h
//
//	@doc:
//		Interface for triggers in the metadata cache
//---------------------------------------------------------------------------

#ifndef GPMD_IMDTrigger_H
#define GPMD_IMDTrigger_H

#include "gpos/base.h"

#include "naucrates/md/IMDCacheObject.h"

namespace gpmd
{
	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@class:
	//		IMDTrigger
	//
	//	@doc:
	//		Interface for triggers in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDTrigger : public IMDCacheObject
	{

		public:

			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtTrigger;
			}

			// does trigger execute on a row-level
			virtual
			BOOL FRow() const = 0;

			// is this a before trigger
			virtual
			BOOL FBefore() const = 0;

			// is this an insert trigger
			virtual
			BOOL FInsert() const = 0;

			// is this a delete trigger
			virtual
			BOOL FDelete() const = 0;

			// is this an update trigger
			virtual
			BOOL FUpdate() const = 0;

			// relation mdid
			virtual
			IMDId *PmdidRel() const = 0;

			// function mdid
			virtual
			IMDId *PmdidFunc() const = 0;

			// is trigger enabled
			virtual
			BOOL FEnabled() const = 0;
	};
}

#endif // !GPMD_IMDTrigger_H

// EOF
