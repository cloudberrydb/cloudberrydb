//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CLogicalBitmapTableGet.h
//
//	@doc:
//		Logical operator for table access via bitmap indexes.
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPOPT_CLogicalBitmapTableGet_H
#define GPOPT_CLogicalBitmapTableGet_H

#include "gpos/base.h"

#include "gpopt/operators/CLogical.h"

namespace gpopt
{
	// fwd declarations
	class CColRefSet;
	class CTableDescriptor;

	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalBitmapTableGet
	//
	//	@doc:
	//		Logical operator for table access via bitmap indexes.
	//
	//---------------------------------------------------------------------------
	class CLogicalBitmapTableGet : public CLogical
	{
		private:
			// table descriptor
			CTableDescriptor *m_ptabdesc;

			// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
			ULONG m_ulOriginOpId;

			// alias for table
			const CName *m_pnameTableAlias;

			// output columns
			CColRefArray *m_pdrgpcrOutput;

			// private copy ctor
			CLogicalBitmapTableGet(const CLogicalBitmapTableGet &);

		public:
			// ctor
			CLogicalBitmapTableGet
				(
				IMemoryPool *mp,
				CTableDescriptor *ptabdesc,
				ULONG ulOriginOpId,
				const CName *pnameTableAlias,
				CColRefArray *pdrgpcrOutput
				);

			// ctor
			// only for transformations
			explicit
			CLogicalBitmapTableGet(IMemoryPool *mp);

			// dtor
			virtual
			~CLogicalBitmapTableGet();

			// table descriptor
			CTableDescriptor *Ptabdesc() const
			{
				return m_ptabdesc;
			}

			// table alias
			const CName *PnameTableAlias()
			{
				return m_pnameTableAlias;
			}

			// array of output column references
			CColRefArray *PdrgpcrOutput() const
			{
				return m_pdrgpcrOutput;
			}

			// identifier
			virtual
			EOperatorId Eopid() const
			{
				return EopLogicalBitmapTableGet;
			}

			// return a string for operator name
			virtual
			const CHAR *SzId() const
			{
				return "CLogicalBitmapTableGet";
			}

			// origin operator id -- gpos::ulong_max if operator was not generated via a transformation
			ULONG UlOriginOpId() const
			{
				return m_ulOriginOpId;
			}

			// operator specific hash function
			virtual
			ULONG HashValue() const;

			// match function
			virtual
			BOOL Matches(COperator *pop) const;

			// sensitivity to order of inputs
			virtual
			BOOL FInputOrderSensitive() const
			{
				return true;
			}

			// return a copy of the operator with remapped columns
			virtual
			COperator *PopCopyWithRemappedColumns(IMemoryPool *mp, UlongToColRefMap *colref_mapping, BOOL must_exist);

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *mp, CExpressionHandle &exprhdl);

			// derive outer references
			virtual
			CColRefSet *PcrsDeriveOuter(IMemoryPool *mp, CExpressionHandle &exprhdl);

			// derive partition consumer info
			virtual
			CPartInfo *PpartinfoDerive
				(
				IMemoryPool *mp,
				CExpressionHandle & //exprhdl
				)
				const
			{
				return GPOS_NEW(mp) CPartInfo(mp);
			}

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint(IMemoryPool *mp, CExpressionHandle &exprhdl) const;

			// derive join depth
			virtual
			ULONG JoinDepth
				(
				IMemoryPool *, // mp
				CExpressionHandle & // exprhdl
				)
				const
			{
				return 1;
			}

			// compute required stat columns of the n-th child
			virtual
			CColRefSet *PcrsStat
				(
				IMemoryPool *mp,
				CExpressionHandle &, // exprhdl
				CColRefSet *, //pcrsInput
				ULONG // child_index
				)
				const
			{
				return GPOS_NEW(mp) CColRefSet(mp);
			}

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *mp) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
				(
				IMemoryPool *mp,
				CExpressionHandle &exprhdl,
				IStatisticsArray *stats_ctxt
				)
				const;

			// stat promise
			virtual
			EStatPromise Esp(CExpressionHandle &) const
			{
				return CLogical::EspHigh;
			}

			// debug print
			virtual
			IOstream &OsPrint(IOstream &) const;

			// conversion
			static
			CLogicalBitmapTableGet *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalBitmapTableGet == pop->Eopid());

				return dynamic_cast<CLogicalBitmapTableGet *>(pop);
			}

	};  // class CLogicalBitmapTableGet
}

#endif // !GPOPT_CLogicalBitmapTableGet_H

// EOF
