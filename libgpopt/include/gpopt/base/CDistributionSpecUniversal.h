//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDistributionSpecUniversal.h
//
//	@doc:
//		Description of a general distribution which reports availability everywhere;
//		Can be used only as a derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CDistributionSpecUniversal_H
#define GPOPT_CDistributionSpecUniversal_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecHashed.h"

namespace gpopt
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDistributionSpecUniversal
	//
	//	@doc:
	//		Class for representing general distribution specification which 
	//		reports availability everywhere.
	//
	//---------------------------------------------------------------------------
	class CDistributionSpecUniversal : public CDistributionSpec
	{
		private:

			// private copy ctor
			CDistributionSpecUniversal(const CDistributionSpecUniversal &);
			
		public:

			//ctor
			CDistributionSpecUniversal()
			{}
			
			// accessor
			virtual 
			EDistributionType Edt() const
			{
				return CDistributionSpec::EdtUniversal;
			}
			
			// does current distribution satisfy the given one
			virtual 
			BOOL FSatisfies
				(
				const CDistributionSpec *pds
				)
				const
			{
				// universal distribution does not satisfy duplicate-sensitive 
				// hash distributions
				if (CDistributionSpec::EdtHashed == pds->Edt() &&
					(CDistributionSpecHashed::PdsConvert(pds))->FDuplicateSensitive())
				{
					return false;
				}
				
				// universal distribution does not satisfy duplicate-sensitive 
				// random distributions
				if (CDistributionSpec::EdtRandom == pds->Edt() &&
					(CDistributionSpecRandom::PdsConvert(pds))->FDuplicateSensitive())
				{
					return false;
				}
				
				if (CDistributionSpec::EdtNonSingleton == pds->Edt())
				{
					// universal distribution does not satisfy non-singleton distribution
					return false;
				}

				return true;
			}
			
			// return true if distribution spec can be required
			virtual
			BOOL FRequirable() const
			{
				return false;
			}

			// does this distribution match the given one
			virtual
			BOOL FMatch
				(
				const CDistributionSpec *pds
				)
				const
			{
				// universal distribution needs to match replicated / singleton requests
				// to avoid generating duplicates
				EDistributionType edt = pds->Edt();
				return (CDistributionSpec::EdtUniversal == edt ||
						CDistributionSpec::EdtSingleton == edt ||
						CDistributionSpec::EdtReplicated == edt);
			}

			// append enforcers to dynamic array for the given plan properties
			virtual
			void AppendEnforcers
				(
				IMemoryPool *, //pmp, 
				CExpressionHandle &, // exprhdl
				CReqdPropPlan *, //prpp,
				DrgPexpr * , // pdrgpexpr, 
				CExpression * // pexpr
				)
			{
				GPOS_ASSERT(!"attempt to enforce UNIVERSAL distribution");
			}

			// print
			virtual
			IOstream &OsPrint(IOstream &os) const
			{
				return os << "UNIVERSAL ";
			}

			// return distribution partitioning type
			virtual
			EDistributionPartitioningType Edpt() const
			{
				return EdptNonPartitioned;
			}

			// conversion function
			static
			CDistributionSpecUniversal *PdsConvert
				(
				CDistributionSpec *pds
				)
			{
				GPOS_ASSERT(NULL != pds);
				GPOS_ASSERT(EdtAny == pds->Edt());

				return dynamic_cast<CDistributionSpecUniversal*>(pds);
			}

	}; // class CDistributionSpecUniversal

}

#endif // !GPOPT_CDistributionSpecUniversal_H

// EOF
