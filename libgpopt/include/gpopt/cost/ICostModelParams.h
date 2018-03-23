//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		ICostModelParams.h
//
//	@doc:
//		Interface for the parameters of the underlying cost model
//---------------------------------------------------------------------------



#ifndef GPOPT_ICostModelParams_H
#define GPOPT_ICostModelParams_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"


#include "naucrates/md/IMDRelation.h"
#include "CCost.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		ICostModelParams
	//
	//	@doc:
	//		Interface for the parameters of the underlying cost model
	//
	//---------------------------------------------------------------------------
	class ICostModelParams : public CRefCount
	{
		public:

			//---------------------------------------------------------------------------
			//	@class:
			//		SCostParam
			//
			//	@doc:
			//		Internal structure to represent cost model parameter
			//
			//---------------------------------------------------------------------------
			struct SCostParam
			{

				private:

					// param identifier
					ULONG m_ulId;

					// param value
					CDouble m_dVal;

					// param lower bound
					CDouble m_dLowerBound;

					// param upper bound
					CDouble m_dUpperBound;

				public:

					// ctor
					SCostParam
						(
						ULONG ulId,
						CDouble dVal,
						CDouble dLowerBound,
						CDouble dUpperBound
						)
						:
						m_ulId(ulId),
						m_dVal(dVal),
						m_dLowerBound(dLowerBound),
						m_dUpperBound(dUpperBound)
					{
						GPOS_ASSERT(dVal >= dLowerBound);
						GPOS_ASSERT(dVal <= dUpperBound);
					}

					// dtor
					virtual
					~SCostParam()
					{};

					// return param identifier
					ULONG UlId() const
					{
						return m_ulId;
					}

					// return value
					CDouble DVal() const
					{
						return m_dVal;
					}

					// return lower bound value
					CDouble DLowerBound() const
					{
						return m_dLowerBound;
					}

					// return upper bound value
					CDouble DUpperBound() const
					{
						return m_dUpperBound;
					}

					BOOL FEquals(SCostParam *pcm) const
					{
						return UlId() == pcm->UlId() && DVal() == pcm->DVal() &&
							   DLowerBound() == pcm->DLowerBound() &&
							   DUpperBound() == pcm->DUpperBound();
					}

			}; // struct SCostParam

			// lookup param by id
			virtual
			SCostParam *PcpLookup(ULONG ulId) const = 0;

			// lookup param by name
			virtual
			SCostParam *PcpLookup(const CHAR *szName) const = 0;

			// set param by id
			virtual
			void SetParam(ULONG ulId, CDouble dVal, CDouble dLowerBound, CDouble dUpperBound) = 0;

			// set param by name
			virtual
			void SetParam(const CHAR *szName, CDouble dVal, CDouble dLowerBound, CDouble dUpperBound) = 0;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const = 0;

			virtual BOOL
			FEquals(ICostModelParams *pcm) const = 0;

			virtual const CHAR *
			SzNameLookup(ULONG ulId) const = 0;
	};
}

#endif // !GPOPT_ICostModelParams_H

// EOF
