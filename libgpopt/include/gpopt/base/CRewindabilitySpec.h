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

			// Rewindability Spec is enforced along two dimensions: Rewindability and Motion Hazard.
			// Following enums, can be perceived together, in required and derive context as follows:
			// 1. RewindableMotion - {ErtRewindable, EmhtMotion}
			//	  require: I require my child to be rewindable and to handle motion hazard (if necessary)
			//	  derive: I am rewindable and I impose a motion hazard (for example, a streaming
			//	  spool with a motion underneath it, will derive this)
			//
			// 2. RewindableNoMotion - {ErtRewindable, EmhtNoMotion}
			//	  require: I require my child to be rewindable and also motion hazard handling is unnecessary.
			//	  derive: I am rewindable and I do not impose motion hazard (any rewindable operator without
			//	  a motion underneath it or a blocking spool with a motion underneath it, will derive this)
			//
			// 3. NotRewindableMotion - {ErtNotRewindable, EmhtMotion}
			//	  require: I do not require my child to be rewindable but it may need to handle motion hazard.
			//	  derive: I am not rewindable and I impose motion hazard (all motions except gather motion will
			//    derive this)
			//
			// 4. NotRewindableNoMotion - {ErtNotRewindable, EmhtNoMotion}
			//	  require: I do not require my child to be rewindable, also, motion hazard handling is unnecessary.
			//	  This is the default rewindability request.
			//	  derive: I am not rewindable and I do not impose any motion hazard.

			enum ERewindabilityType
			{
				ErtRewindable, // rewindability of all intermediate query results

				ErtNotRewindable, // no rewindability

				ErtSentinel
			};

			enum EMotionHazardType
			{
				EmhtMotion, // motion hazard in the tree

				EmhtNoMotion, // no motion hazard in the tree

				EmhtSentinel
            };

		private:

			// rewindability support
			ERewindabilityType m_rewindability;

			// Motion Hazard
			EMotionHazardType m_motion_hazard;

		public:

			// ctor
			explicit
			CRewindabilitySpec(ERewindabilityType rewindability_type, EMotionHazardType motion_hazard);

			// dtor
			virtual
			~CRewindabilitySpec();

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

			ERewindabilityType Ert() const
			{
				return m_rewindability;
			}

			EMotionHazardType Emht() const
			{
				return m_motion_hazard;
			}

			BOOL IsRewindable() const
			{
				return Ert() == ErtRewindable;
			}

			BOOL HasMotionHazard() const
			{
				return Emht() == EmhtMotion;
			}

	}; // class CRewindabilitySpec

}

#endif // !GPOPT_CRewindabilitySpec_H

// EOF
