//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CPropConstraint.h
//
//	@doc:
//		Representation of constraint property
//---------------------------------------------------------------------------
#ifndef GPOPT_CPropConstraint_H
#define GPOPT_CPropConstraint_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CColRefSet.h"
#include "gpopt/base/CConstraint.h"

namespace gpopt
{
	using namespace gpos;

	// forward declaration
	class CConstraint;
	class CExpression;

	//---------------------------------------------------------------------------
	//	@class:
	//		CPropConstraint
	//
	//	@doc:
	//		Representation of constraint property
	//
	//---------------------------------------------------------------------------
	class CPropConstraint : public CRefCount
	{
		private:

			// array of equivalence classes
			DrgPcrs *m_pdrgpcrs;

			// mapping from column to equivalence class
			HMCrCrs *m_phmcrcrs;

			// constraint
			CConstraint *m_pcnstr;

			// hidden copy ctor
			CPropConstraint(const CPropConstraint&);

			// initialize mapping from columns to equivalence classes
			void InitHashMap(IMemoryPool *pmp);

		public:

			// ctor
			CPropConstraint(IMemoryPool *pmp, DrgPcrs *pdrgpcrs, CConstraint *pcnstr);

			// dtor
			virtual
			~CPropConstraint();

			// equivalence classes
			DrgPcrs *PdrgpcrsEquivClasses() const
			{
				return m_pdrgpcrs;
			}

			// mapping
			CColRefSet *PcrsEquivClass
				(
				CColRef *pcr
				)
				const
			{
				return m_phmcrcrs->PtLookup(pcr);
			}

			// constraint
			CConstraint *Pcnstr() const
			{
				return m_pcnstr;
			}

			// is this a contradiction
			BOOL FContradiction() const;

			// scalar expression on given column mapped from all constraints
			// on its equivalent columns
			CExpression *PexprScalarMappedFromEquivCols(IMemoryPool *pmp, CColRef *pcr) const;

			// print
			IOstream &OsPrint(IOstream &) const;

			// debug print
			void DbgPrint() const;

	}; // class CPropConstraint

 	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, CPropConstraint &pc)
	{
		return pc.OsPrint(os);
	}
}

#endif // !GPOPT_CPropConstraint_H

// EOF
