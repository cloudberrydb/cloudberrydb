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
	//			CMDAccessor *pmda = COptCtxt::PoctxtFromTLS()->Pmda();
	//
	//---------------------------------------------------------------------------
	class COptCtxt : public CTaskLocalStorageObject
	{

		private:

			// private copy ctor
			COptCtxt(COptCtxt &);

			// shared memory pool
			IMemoryPool *m_pmp;
		
			// column factory
			CColumnFactory *m_pcf;

			// metadata accessor;
			CMDAccessor *m_pmda;

			// cost model
			ICostModel *m_pcm;

			// constant expression evaluator
			IConstExprEvaluator *m_pceeval;

			// comparator between IDatum instances
			IComparator *m_pcomp;

			// atomic counter for generating part index ids
			CAtomicULONG m_auPartId;

			// global CTE information
			CCTEInfo *m_pcteinfo;

			// system columns required in query output
			DrgPcr *m_pdrgpcrSystemCols;

			// optimizer configurations
			COptimizerConfig *m_poconf;

			// whether or not we are optimizing a DML query
			BOOL m_fDMLQuery;

			// value for the first valid part id
			static
			ULONG m_ulFirstValidPartId;

		public:

			// ctor
			COptCtxt
				(
				IMemoryPool *pmp,
				CColumnFactory *pcf,
				CMDAccessor *pmda,
				IConstExprEvaluator *pceeval,
				COptimizerConfig *poconf
				);

			// dtor
			virtual
			~COptCtxt();

			// memory pool accessor
			IMemoryPool *Pmp() const
			{
				return m_pmp;
			}
			
			// optimizer configurations
			COptimizerConfig *Poconf() const
			{
				return m_poconf;
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
			ICostModel *Pcm() const
			{
				return m_pcm;
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
				return m_auPartId.TIncr();
			}
			
			// required system columns
			DrgPcr *PdrgpcrSystemCols() const
			{
				return m_pdrgpcrSystemCols;
			}

			// set required system columns
			void SetReqdSystemCols
				(
				DrgPcr *pdrgpcrSystemCols
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
						IMemoryPool *pmp,
						CMDAccessor *pmda,
						IConstExprEvaluator *pceeval,
						COptimizerConfig *poconf
						);
			
			// shorthand to retrieve opt context from TLS
			inline
			static
			COptCtxt *PoctxtFromTLS()
			{
				return reinterpret_cast<COptCtxt*>(ITask::PtskSelf()->Tls().Ptlsobj(CTaskLocalStorage::EtlsidxOptCtxt));
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
