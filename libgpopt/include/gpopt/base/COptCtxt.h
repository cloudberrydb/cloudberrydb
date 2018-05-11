//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		COptCtxt.h
//
//	@doc:
//		Optimizer context object; contains all global objects pertaining to
//		one optimization
//---------------------------------------------------------------------------
#ifndef GPOPT_COptCtxt_H
#define GPOPT_COptCtxt_H

#include "gpos/base.h"
#include "gpos/task/CTaskLocalStorageObject.h"

#include "gpopt/base/CColumnFactory.h"
#include "gpopt/base/CCTEInfo.h"
#include "gpopt/base/IComparator.h"
#include "gpopt/mdcache/CMDAccessor.h"

namespace gpopt
{
	using namespace gpos;
	
	// forward declarations
	class CColRefSet;
	class COptimizerConfig;
	class ICostModel;
	class IConstExprEvaluator;

	//---------------------------------------------------------------------------
	//	@class:
	//		COptCtxt
	//
	//	@doc:
	//		"Optimizer Context" is a container of global objects (mostly
	//		singletons) that are needed by the optimizer.
	//
	//		A COptCtxt object is instantiated in COptimizer::PdxlnOptimize() via
	//		COptCtxt::PoctxtCreate() and stored as a task local object. The global
	//		information contained in it can be accessed by calling
	//		COptCtxt::PoctxtFromTLS(), instead of passing a pointer to it all
	//		around. For example to get the global CMDAccessor:
	//			CMDAccessor *md_accessor = COptCtxt::PoctxtFromTLS()->Pmda();
	//
	//---------------------------------------------------------------------------
	class COptCtxt : public CTaskLocalStorageObject
	{

		private:

			// private copy ctor
			COptCtxt(COptCtxt &);

			// shared memory pool
			IMemoryPool *m_mp;
		
			// column factory
			CColumnFactory *m_pcf;

			// metadata accessor;
			CMDAccessor *m_pmda;

			// cost model
			ICostModel *m_cost_model;

			// constant expression evaluator
			IConstExprEvaluator *m_pceeval;

			// comparator between IDatum instances
			IComparator *m_pcomp;

			// atomic counter for generating part index ids
			CAtomicULONG m_auPartId;

			// global CTE information
			CCTEInfo *m_pcteinfo;

			// system columns required in query output
			CColRefArray *m_pdrgpcrSystemCols;

			// optimizer configurations
			COptimizerConfig *m_optimizer_config;

			// whether or not we are optimizing a DML query
			BOOL m_fDMLQuery;

			// value for the first valid part id
			static
			ULONG m_ulFirstValidPartId;

		public:

			// ctor
			COptCtxt
				(
				IMemoryPool *mp,
				CColumnFactory *col_factory,
				CMDAccessor *md_accessor,
				IConstExprEvaluator *pceeval,
				COptimizerConfig *optimizer_config
				);

			// dtor
			virtual
			~COptCtxt();

			// memory pool accessor
			IMemoryPool *Pmp() const
			{
				return m_mp;
			}
			
			// optimizer configurations
			COptimizerConfig *GetOptimizerConfig() const
			{
				return m_optimizer_config;
			}

			// are we optimizing a DML query
			BOOL FDMLQuery() const
			{
				return m_fDMLQuery;
			}

			// set the DML flag
			void MarkDMLQuery
				(
				BOOL fDMLQuery
				)
			{
				m_fDMLQuery = fDMLQuery;
			}

			// column factory accessor
			CColumnFactory *Pcf() const
			{
				return m_pcf;
			}
			
			// metadata accessor
			CMDAccessor *Pmda() const
			{
				return m_pmda;
			}

			// cost model accessor
			ICostModel *GetCostModel() const
			{
				return m_cost_model;
			}

			// constant expression evaluator
			IConstExprEvaluator *Pceeval()
			{
				return m_pceeval;
			}

			// comparator
			const IComparator *Pcomp()
			{
				return m_pcomp;
			}

			// cte info
			CCTEInfo *Pcteinfo()
			{
				return m_pcteinfo;
			}

			// return a new part index id
			ULONG UlPartIndexNextVal()
			{
				return m_auPartId.Incr();
			}
			
			// required system columns
			CColRefArray *PdrgpcrSystemCols() const
			{
				return m_pdrgpcrSystemCols;
			}

			// set required system columns
			void SetReqdSystemCols
				(
				CColRefArray *pdrgpcrSystemCols
				)
			{
				GPOS_ASSERT(NULL != pdrgpcrSystemCols);

				CRefCount::SafeRelease(m_pdrgpcrSystemCols);
				m_pdrgpcrSystemCols = pdrgpcrSystemCols;
			}

			// factory method
			static
			COptCtxt *PoctxtCreate
						(
						IMemoryPool *mp,
						CMDAccessor *md_accessor,
						IConstExprEvaluator *pceeval,
						COptimizerConfig *optimizer_config
						);
			
			// shorthand to retrieve opt context from TLS
			inline
			static
			COptCtxt *PoctxtFromTLS()
			{
				return reinterpret_cast<COptCtxt*>(ITask::Self()->GetTls().Get(CTaskLocalStorage::EtlsidxOptCtxt));
			}
			
			// return true if all enforcers are enabled
			static
			BOOL FAllEnforcersEnabled();

#ifdef GPOS_DEBUG
			virtual
			IOstream &OsPrint(IOstream &) const;
#endif // GPOS_DEBUG

	}; // class COptCtxt
}


#endif // !GPOPT_COptCtxt_H

// EOF
