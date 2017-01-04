//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CAutoLogger.h
//
//	@doc:
//		Auto object for replacing the logger for output/error.
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoLogger_H
#define GPOS_CAutoLogger_H

#include "gpos/base.h"
#include "gpos/task/CTaskContext.h"

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoLogger
	//
	//	@doc:
	//		Auto object for replacing the logger for outpur/error.
	//
	//---------------------------------------------------------------------------
	class CAutoLogger : public CStackObject
	{

		private:

			// old logger
			ILogger *m_ploggerOld;

			// flag indicating if logger is used for error logging
			BOOL m_fError;

			// private copy ctor
			CAutoLogger(const CAutoLogger &);

		public:

			// ctor
			CAutoLogger
				(
				ILogger *plogger,
				BOOL fError
				)
				:
				m_ploggerOld(NULL),
				m_fError(fError)
			{
				GPOS_ASSERT(NULL != plogger);

				ITask *ptsk = ITask::PtskSelf();
				GPOS_ASSERT(NULL != ptsk);

				if (m_fError)
				{
					m_ploggerOld = ptsk->PlogErr();
					ptsk->Ptskctxt()->SetLogErr(plogger);
				}
				else
				{
					m_ploggerOld = ptsk->PlogOut();
					ptsk->Ptskctxt()->SetLogOut(plogger);
				}
			}

			// dtor
			~CAutoLogger()
			{
				ITask *ptsk = ITask::PtskSelf();
				GPOS_ASSERT(NULL != ptsk);

				if (m_fError)
				{
					ptsk->Ptskctxt()->SetLogErr(m_ploggerOld);
				}
				else
				{
					ptsk->Ptskctxt()->SetLogOut(m_ploggerOld);
				}
			}

	}; // class CAutoLogger
}

#endif // !GPOS_CAutoLogger_H

// EOF

