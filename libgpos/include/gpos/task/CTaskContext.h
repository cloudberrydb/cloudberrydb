//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CTaskContext.h
//
//	@doc:
//		Context object for task; holds configurable state of worker, e.g.
//		GUCs, trace options, output streams etc.; by default inherited from 
//		parent task
//---------------------------------------------------------------------------
#ifndef GPOS_CTaskContext_H
#define GPOS_CTaskContext_H

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"

namespace gpos
{
	class CTaskContext
	{
		// trace flag iterator needs access to trace vector
		friend class CTraceFlagIter;

		private:
		
			// trace vector
			CBitSet *m_pbs;
		
			// output log abstraction
			ILogger *m_plogOut;
			
			// error log abstraction
			ILogger *m_plogErr;
			
			// locale of messages
			ELocale m_eloc;
			
		public:
				
			// basic ctor; used only for the main worker
			CTaskContext(IMemoryPool *pmp);

			// copy ctor
			// used to inherit parent task's context
			CTaskContext(IMemoryPool *pmp, const CTaskContext &tskctxt);
			
			// dtor
			~CTaskContext();

			// accessors
			inline
			ILogger *PlogOut() const
			{
				return m_plogOut;
			}
			
			inline
			ILogger *PlogErr() const
			{
				return m_plogErr;
			}
			
			void SetLogOut(ILogger *plogOut)
			{
				GPOS_ASSERT(NULL != plogOut);
				m_plogOut = plogOut;
			}

			void SetLogErr(ILogger *plogErr)
			{
				GPOS_ASSERT(NULL != plogErr);
				m_plogErr = plogErr;
			}

			// set trace flag
			BOOL FTrace(ULONG ulTrace, BOOL fVal);
			
			// test if tracing on
			inline
			BOOL FTrace
				(
				ULONG ulTrace
				)
			{
				return m_pbs->FBit(ulTrace);
			}
		
			// locale
			ELocale Eloc() const
			{
				return m_eloc;
			}
			
			void SetEloc
				(
				ELocale eloc
				)
			{
				m_eloc = eloc;
			}

			CBitSet *PbsCopyTraceFlags(IMemoryPool *pmp) const
			{
				return GPOS_NEW(pmp) CBitSet(pmp, *m_pbs);
			}
		
	}; // class CTaskContext
}

#endif // !GPOS_CTaskContext_H

// EOF

