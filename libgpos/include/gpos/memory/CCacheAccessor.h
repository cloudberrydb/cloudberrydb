//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC CORP.
//
//	@filename:
//		CCacheAccessor.h
//
//	@doc:
//		Definition of cache accessor class.
//.
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CCACHEACCESSOR_H_
#define GPOS_CCACHEACCESSOR_H_

#include "gpos/memory/CCacheAccessorBase.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CCacheAccessor
	//
	//	@doc:
	//		Definition of CCacheAccessor;
	//		provides access to parent class CCacheAccessorBase;
	//		template parameters are T (object type) and K (key type)
	//---------------------------------------------------------------------------
	template <class T, class K>
	class CCacheAccessor : public CCacheAccessorBase
	{

		public:

			//ctor
			CCacheAccessor
				(
				CCache *pcache
				)
				:
				CCacheAccessorBase(pcache)
			{

			}

			// inserts a new object; if insertion succeeds, the function
			// returns the value of passed object; otherwise the first
			// existing object with a matching key is returned; the function
			// pins either the newly inserted object, or the already existing
			// object
			T* PtInsert
				(
				K* pkey,
				T* pval
				)
			{
				return static_cast<T*>
					(CCacheAccessorBase::PvInsert(pkey, pval));
			}

			// returns the object currently held by the accessor
			T* PtVal() const
			{
				return static_cast<T*>
					(CCacheAccessorBase::PvVal());
			}

			// gets the next object following the current object with
			// the same key
			T* PtNext()
			{
				return static_cast<T*>
					(CCacheAccessorBase::PvNext());
			}

	};  // CCacheAccessor

} //namespace gpos

#endif // GPOS_CCACHEACCESSOR_H_

// EOF
