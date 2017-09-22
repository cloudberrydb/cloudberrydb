//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CConstraint.h
//
//	@doc:
//		Base class for representing constraints
//---------------------------------------------------------------------------
#ifndef GPOPT_CConstraint_H
#define GPOPT_CConstraint_H

#include "gpos/base.h"
#include "gpos/types.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/base/CRange.h"

namespace gpopt
{
	using namespace gpos;

	// fwd declaration
	class CExpression;
	class CConstraint;
	class CRange;

	// constraint array
	typedef CDynamicPtrArray<CConstraint, CleanupRelease> DrgPcnstr;

	// hash map mapping CColRef -> DrgPcnstr
	typedef CHashMap<CColRef, DrgPcnstr, gpos::UlHash<CColRef>, gpos::FEqual<CColRef>,
					CleanupNULL<CColRef>, CleanupRelease<DrgPcnstr> > HMColConstr;

	// mapping CConstraint -> BOOL to cache previous containment queries,
	// we use pointer equality here for fast map lookup -- since we do shallow comparison, we do not take ownership
	// of pointer values
	typedef CHashMap<CConstraint, BOOL, gpos::UlHashPtr<CConstraint>, gpos::FEqualPtr<CConstraint>,
					CleanupNULL<CConstraint>, CleanupNULL<BOOL> > HMConstraintContainment;

	// hash map mapping ULONG -> CConstraint
	typedef CHashMap<ULONG, CConstraint, gpos::UlHash<ULONG>, gpos::FEqual<ULONG>,
					CleanupDelete<ULONG>, CleanupRelease<CConstraint> > HMUlCnstr;

	//---------------------------------------------------------------------------
	//	@class:
	//		CConstraint
	//
	//	@doc:
	//		Base class for representing constraints
	//
	//---------------------------------------------------------------------------
	class CConstraint : public CRefCount
	{
		public:

			enum EConstraintType
			{
				EctInterval, // a single interval on a single columns
				EctConjunction, // a set of ANDed constraints
				EctDisjunction, // a set of ORed constraints
				EctNegation // a negated constraint
			};

		private:

			// containment map
			HMConstraintContainment *m_phmcontain;

			// constant true
			static
			BOOL m_fTrue;

			// constant false
			static
			BOOL m_fFalse;

			// hidden copy ctor
			CConstraint(const CConstraint&);

			// return address of static BOOL constant based on passed BOOL value
			static
			BOOL *PfVal
				(
				BOOL fVal
				)
			{
				if (fVal)
				{
					return &m_fTrue;
				}

				return &m_fFalse;
			}

			// add column as a new equivalence class, if it is not already in one of the
			// existing equivalence classes
			static
			void AddColumnToEquivClasses(IMemoryPool *pmp, const CColRef *pcr, DrgPcrs **ppdrgpcrs);

			// create constraint from scalar comparison
			static
			CConstraint *PcnstrFromScalarCmp(IMemoryPool *pmp, CExpression *pexpr, DrgPcrs **ppdrgpcrs);

			// create constraint from scalar boolean expression
			static
			CConstraint *PcnstrFromScalarBoolOp(IMemoryPool *pmp, CExpression *pexpr, DrgPcrs **ppdrgpcrs);

			// create conjunction/disjunction from array of constraints
			static
			CConstraint *PcnstrConjDisj(IMemoryPool *pmp, DrgPcnstr *pdrgpcnstr, BOOL fConj);

		protected:

			// memory pool -- used for local computations
			IMemoryPool *m_pmp;

			// columns used in this constraint
			CColRefSet *m_pcrsUsed;

			// equivalent scalar expression
			CExpression *m_pexprScalar;

			// print
			IOstream &PrintConjunctionDisjunction
						(
						IOstream &os,
						DrgPcnstr *pdrgpcnstr
						)
						const;

			// construct a conjunction or disjunction scalar expression from an
			// array of constraints
			CExpression *PexprScalarConjDisj(IMemoryPool *pmp, DrgPcnstr *pdrgpcnstr, BOOL fConj) const;

			// flatten an array of constraints to be used as constraint children
			DrgPcnstr *PdrgpcnstrFlatten(IMemoryPool *pmp, DrgPcnstr *pdrgpcnstr, EConstraintType ect) const;

			// combine any two or more constraints that reference only one particular column
			DrgPcnstr *PdrgpcnstrDeduplicate(IMemoryPool *pmp, DrgPcnstr *pdrgpcnstr, EConstraintType ect) const;

			// mapping between columns and arrays of constraints
			HMColConstr *Phmcolconstr(IMemoryPool *pmp, CColRefSet *pcrs, DrgPcnstr *pdrgpcnstr) const;

			// return a copy of the conjunction/disjunction constraint for a different column
			CConstraint *PcnstrConjDisjRemapForColumn
							(
							IMemoryPool *pmp,
							CColRef *pcr,
							DrgPcnstr *pdrgpcnstr,
							BOOL fConj
							)
							const;

			// create constraint from scalar array comparison expression
			static
			CConstraint *PcnstrFromScalarArrayCmp(IMemoryPool *pmp, CExpression *pexpr, CColRef *pcr);

		public:

			// ctor
			explicit
			CConstraint(IMemoryPool *pmp);

			// dtor
			virtual
			~CConstraint();

			// constraint type accessor
			virtual
			EConstraintType Ect() const = 0;

			// is this constraint a contradiction
			virtual
			BOOL FContradiction() const = 0;

			// is this constraint unbounded
			virtual
			BOOL FUnbounded() const
			{
				return false;
			}
			
			// does the current constraint contain the given one
			virtual
			BOOL FContains(CConstraint *pcnstr);

			// equality function
			virtual
			BOOL FEquals(CConstraint *pcnstr);

			// columns in this constraint
			virtual
			CColRefSet *PcrsUsed() const
			{
				return m_pcrsUsed;
			}

			// scalar expression
			virtual
			CExpression *PexprScalar(IMemoryPool *pmp) = 0;

			// check if there is a constraint on the given column
			virtual
			BOOL FConstraint(const CColRef *pcr) const = 0;

			// return a copy of the constraint with remapped columns
			virtual
			CConstraint *PcnstrCopyWithRemappedColumns(IMemoryPool *pmp, HMUlCr *phmulcr, BOOL fMustExist) = 0;

			// return constraint on a given column
			virtual
			CConstraint *Pcnstr
							(
							IMemoryPool *, //pmp,
							const CColRef * //pcr
							)
			{
				return NULL;
			}

			// return constraint on a given set of columns
			virtual
			CConstraint *Pcnstr
							(
							IMemoryPool *, //pmp,
							CColRefSet * //pcrs
							)
			{
				return NULL;
			}

			// return a clone of the constraint for a different column
			virtual
			CConstraint *PcnstrRemapForColumn(IMemoryPool *pmp, CColRef *pcr) const = 0;

			// print
			virtual
			IOstream &OsPrint
						(
						IOstream &os
						)
						const = 0;

			// create constraint from scalar expression and pass back any discovered
			// equivalence classes
			static
			CConstraint *PcnstrFromScalarExpr
							(
							IMemoryPool *pmp,
							CExpression *pexpr,
							DrgPcrs **ppdrgpcrs
							);

			// create conjunction from array of constraints
			static
			CConstraint *PcnstrConjunction(IMemoryPool *pmp, DrgPcnstr *pdrgpcnstr);

			// create disjunction from array of constraints
			static
			CConstraint *PcnstrDisjunction(IMemoryPool *pmp, DrgPcnstr *pdrgpcnstr);

			// merge equivalence classes coming from children of a bool op
			static
			DrgPcrs *PdrgpcrsMergeFromBoolOp(IMemoryPool *pmp, CExpression *pexpr, DrgPcrs *pdrgpcrsFst, DrgPcrs *pdrgpcrsSnd);

			// subset of the given constraints, which reference the given column
			static
			DrgPcnstr *PdrgpcnstrOnColumn(IMemoryPool *pmp, DrgPcnstr *pdrgpcnstr, CColRef *pcr, BOOL fExclusive);
#ifdef GPOS_DEBUG
			void DbgPrint() const;
#endif  // GPOS_DEBUG

	}; // class CConstraint

	// shorthand for printing, pointer.
	inline
	IOstream &operator << (IOstream &os, const CConstraint *cnstr)
	{
		return cnstr->OsPrint(os);
	}
	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, const CConstraint &cnstr)
	{
		return cnstr.OsPrint(os);
	}
}

#endif // !GPOPT_CConstraint_H

// EOF
