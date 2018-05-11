//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CRewindabilitySpec.h
//
//	@doc:
//		Rewindability of intermediate query results;
//		Can be used as required or derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CRewindabilitySpec_H
#define GPOPT_CRewindabilitySpec_H

#include "gpos/base.h"
#include "gpopt/base/CPropSpec.h"
#include "gpos/common/CRefCount.h"


namespace gpopt
{
	using namespace gpos;


	//---------------------------------------------------------------------------
	//	@class:
	//		CRewindabilitySpec
	//
	//	@doc:
	//		Rewindability specification
	//
	//---------------------------------------------------------------------------
	class CRewindabilitySpec : public CPropSpec
	{

		public:

			enum ERewindabilityType
			{
				ErtGeneral,			// rewindability of all intermediate query results
				ErtMarkRestore,		// rewindability of a subset of intermediate query results
				ErtNone,			// no rewindability

				ErtSentinel
			};

		private:

			// rewindability support
			ERewindabilityType m_ert;

		public:

			// ctor
			explicit
			CRewindabilitySpec(ERewindabilityType ert);

			// dtor
			virtual
			~CRewindabilitySpec();

			// accessor of rewindablility type
			ERewindabilityType Ert() const
			{
				return m_ert;
			}

			// check if rewindability specs match
 			BOOL Matches(const CRewindabilitySpec *prs) const;

			// check if rewindability spec satisfies a req'd rewindability spec
			BOOL FSatisfies(const CRewindabilitySpec *prs) const;

			// append enforcers to dynamic array for the given plan properties
			virtual
			void AppendEnforcers(IMemoryPool *mp, CExpressionHandle &exprhdl, CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr, CExpression *pexpr);

			// hash function
			virtual
			ULONG HashValue() const;

			// extract columns used by the rewindability spec
			virtual
			CColRefSet *PcrsUsed
				(
				IMemoryPool *mp
				)
				const
			{
				// return an empty set
				return GPOS_NEW(mp) CColRefSet(mp);
			}

			// property type
			virtual
			EPropSpecType Epst() const
			{
				return EpstRewindability;
			}

			// print
			IOstream &OsPrint(IOstream &os) const;

	}; // class CRewindabilitySpec

}

#endif // !GPOPT_CRewindabilitySpec_H

// EOF
