//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraintNegation.h
//
//	@doc:
//		Representation of a negation constraint
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraintNegation_H
#define GPOPT_CConstraintNegation_H

#include "gpos/base.h"

#include "gpopt/base/CConstraint.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CConstraintNegation
	//
	//	@doc:
	//		Representation of a negation constraint
	//
	//---------------------------------------------------------------------------
	class CConstraintNegation : public CConstraint
	{
		private:

			// child constraint
			CConstraint *m_pcnstr;

			// hidden copy ctor
			CConstraintNegation(const CConstraintNegation&);

		public:

			// ctor
			CConstraintNegation(IMemoryPool *pmp, CConstraint *pcnstr);

			// dtor
			virtual
			~CConstraintNegation();

			// constraint type accessor
			virtual
			EConstraintType Ect() const
			{
				return CConstraint::EctNegation;
			}

			// child constraint
			CConstraint *PcnstrChild() const
			{
				return m_pcnstr;
			}

			// is this constraint a contradiction
			virtual
			BOOL FContradiction() const
			{
				return m_pcnstr->FUnbounded();
			}

			// is this constraint unbounded
			virtual
			BOOL FUnbounded() const
			{
				return m_pcnstr->FContradiction();
			}

			// scalar expression
			virtual
			CExpression *PexprScalar(IMemoryPool *pmp);

			// check if there is a constraint on the given column
			virtual
			BOOL FConstraint
					(
					const CColRef *pcr
					)
					const
			{
				return m_pcnstr->FConstraint(pcr);
			}

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
			IOstream &OsPrint(IOstream &os) const;

	}; // class CConstraintNegation
}

#endif // !GPOPT_CConstraintNegation_H

// EOF
