//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintConjunction.h
//
//	@doc:
//		Representation of a conjunction constraint. A conjunction is a number
//		of ANDed constraints
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraintConjunction_H
#define GPOPT_CConstraintConjunction_H

#include "gpos/base.h"

#include "gpopt/base/CConstraint.h"
#include "gpopt/base/CRange.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CConstraintConjunction
	//
	//	@doc:
	//		Representation of a conjunction constraint
	//
	//---------------------------------------------------------------------------
	class CConstraintConjunction : public CConstraint
	{
		private:

			// array of constraints
			DrgPcnstr *m_pdrgpcnstr;

			// mapping colref -> array of child constraints
			HMColConstr *m_phmcolconstr;

			// hidden copy ctor
			CConstraintConjunction(const CConstraintConjunction&);

		public:

			// ctor
			CConstraintConjunction(IMemoryPool *pmp, DrgPcnstr *pdrgpcnstr);

			// dtor
			virtual
			~CConstraintConjunction();

			// constraint type accessor
			virtual
			EConstraintType Ect() const
			{
				return CConstraint::EctConjunction;
			}

			// all constraints in conjunction
			DrgPcnstr *Pdrgpcnstr() const
			{
				return m_pdrgpcnstr;
			}

			// is this constraint a contradiction
			virtual
			BOOL FContradiction() const;

			// scalar expression
			virtual
			CExpression *PexprScalar(IMemoryPool *pmp);

			// check if there is a constraint on the given column
			virtual
			BOOL FConstraint(const CColRef *pcr) const;

			// return a copy of the constraint with remapped columns
			virtual
			CConstraint *PcnstrCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist);

			// return constraint on a given column
			virtual
			CConstraint *Pcnstr(IMemoryPool *pmp, const CColRef *pcr);

			// return constraint on a given column set
			virtual
			CConstraint *Pcnstr(IMemoryPool *pmp, CColRefSet *pcrs);

			// return a clone of the constraint for a different column
			virtual
			CConstraint *PcnstrRemapForColumn(IMemoryPool *pmp, CColRef *pcr) const;

			// print
			virtual
			IOstream &OsPrint
						(
						IOstream &os
						)
						const
			{
				return PrintConjunctionDisjunction(os, m_pdrgpcnstr);
			}

	}; // class CConstraintConjunction
}

#endif // !GPOPT_CConstraintConjunction_H

// EOF
