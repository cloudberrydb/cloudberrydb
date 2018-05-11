//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 - 2011 EMC CORP.
//
//	@filename:
//		CDrvdPropRelational.h
//
//	@doc:
//		Derived logical properties
//---------------------------------------------------------------------------
#ifndef GPOPT_CDrvdPropRelational_H
#define GPOPT_CDrvdPropRelational_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CColRef.h"
#include "gpopt/base/CDrvdProp.h"
#include "gpopt/base/CMaxCard.h"
#include "gpopt/base/CFunctionalDependency.h"
#include "gpopt/base/CPropConstraint.h"
#include "gpopt/base/CFunctionProp.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declaration
	class CExpressionHandle;
	class CColRefSet;
	class CReqdPropPlan;
	class CKeyCollection;
	class CPartIndexMap;
	class CPropConstraint;
	class CPartInfo;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDrvdPropRelational
	//
	//	@doc:
	//		Derived logical properties container.
	//
	//		These are properties than can be inferred from logical expressions or
	//		Memo groups. This includes output columns, outer references, primary
	//		keys. These properties hold regardless of the physical implementation
	//		of an expression.
	//
	//---------------------------------------------------------------------------
	class CDrvdPropRelational : public DrvdPropArray
	{

		private:

			// output columns
			CColRefSet *m_pcrsOutput;

			// columns not defined in the underlying operator tree
			CColRefSet *m_pcrsOuter;
			
			// output columns that do not allow null values
			CColRefSet *m_pcrsNotNull;

			// columns from the inner child of a correlated-apply expression that can be used above the apply expression
			CColRefSet *m_pcrsCorrelatedApply;

			// key collection
			CKeyCollection *m_pkc;
			
			// functional dependencies
			CFunctionalDependencyArray *m_pdrgpfd;
			
			// max card
			CMaxCard m_maxcard;
			
			// join depth (number of relations in underlying tree)
			ULONG m_ulJoinDepth;

			// partition table consumers
			CPartInfo *m_ppartinfo;

			// constraint property
			CPropConstraint *m_ppc;

			// function properties
			CFunctionProp *m_pfp;

			// true if all logical operators in the group are of type CLogicalDynamicGet,
			// and the dynamic get has partial indexes
			BOOL m_fHasPartialIndexes;

			// private copy ctor
			CDrvdPropRelational(const CDrvdPropRelational &);

			// helper for getting applicable FDs from child
			static
			CFunctionalDependencyArray *PdrgpfdChild(IMemoryPool *mp, ULONG child_index, CExpressionHandle &exprhdl);

			// helper for creating local FDs
			static
			CFunctionalDependencyArray *PdrgpfdLocal(IMemoryPool *mp, CExpressionHandle &exprhdl);

			// helper for deriving FD's
			static
			CFunctionalDependencyArray *Pdrgpfd(IMemoryPool *mp, CExpressionHandle &exprhdl);

		public:

			// ctor
			CDrvdPropRelational();

			// dtor
			virtual 
			~CDrvdPropRelational();

			// type of properties
			virtual
			EPropType Ept()
			{
				return EptRelational;
			}

			// derivation function
			void Derive(IMemoryPool *mp, CExpressionHandle &exprhdl, CDrvdPropCtxt *pdpctxt);

			// output columns
			CColRefSet *PcrsOutput() const
			{
				return m_pcrsOutput;
			}

			// outer references
			CColRefSet *PcrsOuter() const
			{
				return m_pcrsOuter;
			}
			
			// nullable columns
			CColRefSet *PcrsNotNull() const
			{
				return m_pcrsNotNull;
			}

			// columns from the inner child of a correlated-apply expression that can be used above the apply expression
			CColRefSet *PcrsCorrelatedApply() const
			{
				return m_pcrsCorrelatedApply;
			}

			// key collection
			CKeyCollection *Pkc() const
			{
				return m_pkc;
			}
		
			// functional dependencies
			CFunctionalDependencyArray *Pdrgpfd() const
			{
				return m_pdrgpfd;
			}

			// check if relation has a key
			BOOL FHasKey() const
			{
				return NULL != m_pkc;
			}

			// max cardinality
			CMaxCard Maxcard() const
			{
				return m_maxcard;
			}

			// join depth
			ULONG JoinDepth() const
			{
				return m_ulJoinDepth;
			}

			// partition consumers
			CPartInfo *Ppartinfo() const
			{
				return m_ppartinfo;
			}

			// constraint property
			CPropConstraint *Ppc() const
			{
				return m_ppc;
			}

			// function properties
			CFunctionProp *Pfp() const
			{
				return m_pfp;
			}

			// has partial indexes
			BOOL FHasPartialIndexes() const
			{
				return m_fHasPartialIndexes;
			}

			// shorthand for conversion
			static
			CDrvdPropRelational *GetRelationalProperties(DrvdPropArray *pdp);

			// check for satisfying required plan properties
			virtual
			BOOL FSatisfies(const CReqdPropPlan *prpp) const;

			// print function
			virtual
			IOstream &OsPrint(IOstream &os) const;

	}; // class CDrvdPropRelational

}


#endif // !GPOPT_CDrvdPropRelational_H

// EOF
