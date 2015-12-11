//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CLogicalProject.h
//
//	@doc:
//		Project operator
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CLogicalProject_H
#define GPOS_CLogicalProject_H

#include "gpos/base.h"

#include "gpopt/operators/CExpressionHandle.h"
#include "gpopt/operators/CLogicalUnary.h"

namespace gpopt
{
	// fwd declaration
	class CColRefSet;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CLogicalProject
	//
	//	@doc:
	//		Project operator
	//
	//---------------------------------------------------------------------------
	class CLogicalProject : public CLogicalUnary
	{
		private:

			// private copy ctor
			CLogicalProject(const CLogicalProject &);

			// return equivalence class from scalar ident project element
			static
			DrgPcrs *PdrgpcrsEquivClassFromScIdent(IMemoryPool *pmp, CExpression *pexprPrEl);

			// extract constraint from scalar constant project element
			static
			void ExtractConstraintFromScConst
					(
					IMemoryPool *pmp,
					CExpression *pexprPrEl,
					DrgPcnstr *pdrgpcnstr,
					DrgPcrs *pdrgpcrs
					);

		public:

			// ctor
			explicit
			CLogicalProject(IMemoryPool *pmp);

			// dtor
			virtual
			~CLogicalProject()
			{}

			// ident accessors
			virtual 
			EOperatorId Eopid() const
			{
				return EopLogicalProject;
			}
			
			virtual 
			const CHAR *SzId() const
			{
				return "CLogicalProject";
			}

			//-------------------------------------------------------------------------------------
			// Derived Relational Properties
			//-------------------------------------------------------------------------------------

			// derive output columns
			virtual
			CColRefSet *PcrsDeriveOutput(IMemoryPool *pmp, CExpressionHandle &exprhdl);
			
			// dervive keys
			virtual 
			CKeyCollection *PkcDeriveKeys(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;
					
			// derive max card
			virtual
			CMaxCard Maxcard(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			// derive constraint property
			virtual
			CPropConstraint *PpcDeriveConstraint(IMemoryPool *pmp, CExpressionHandle &exprhdl) const;

			//-------------------------------------------------------------------------------------
			// Transformations
			//-------------------------------------------------------------------------------------

			// candidate set of xforms
			virtual
			CXformSet *PxfsCandidates(IMemoryPool *pmp) const;

			// derive statistics
			virtual
			IStatistics *PstatsDerive
						(
						IMemoryPool *pmp,
						CExpressionHandle &exprhdl,
						DrgPstat *pdrgpstatCtxt
						)
						const;

			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------
			//-------------------------------------------------------------------------------------

			// conversion function
			static
			CLogicalProject *PopConvert
				(
				COperator *pop
				)
			{
				GPOS_ASSERT(NULL != pop);
				GPOS_ASSERT(EopLogicalProject == pop->Eopid());
				
				return dynamic_cast<CLogicalProject*>(pop);
			}

	}; // class CLogicalProject

}

#endif // !GPOS_CLogicalProject_H

// EOF
