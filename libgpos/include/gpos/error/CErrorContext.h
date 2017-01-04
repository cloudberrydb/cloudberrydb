//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CErrorContext.h
//
//	@doc:
//		Error context to record error message, stack, etc.
//---------------------------------------------------------------------------
#ifndef GPOS_CErrorContext_H
#define GPOS_CErrorContext_H

#include "gpos/common/CStackDescriptor.h"
#include "gpos/error/CException.h"
#include "gpos/error/CMiniDumper.h"
#include "gpos/error/CSerializable.h"
#include "gpos/error/IErrorContext.h"
#include "gpos/string/CWStringStatic.h"
#include "gpos/sync/CAutoSpinlock.h"

#define GPOS_ERROR_MESSAGE_BUFFER_SIZE	(4 * 1024)

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CErrorContext
	//
	//	@doc:
	//		Context object, owned by Task
	//
	//---------------------------------------------------------------------------
	class CErrorContext : public IErrorContext
	{

		private:

			// copy of original exception
			CException m_exc;

			// exception severity
			ULONG m_ulSev;

			// flag to indicate if handled yet
			BOOL m_fPending;
			
			// flag to indicate if handled yet
			BOOL m_fRethrow;

			// error message buffer
			WCHAR m_wsz[GPOS_ERROR_MESSAGE_BUFFER_SIZE];
			
			// system error message buffer
			CHAR m_sz[GPOS_ERROR_MESSAGE_BUFFER_SIZE];

			// string with static buffer allocation
			CWStringStatic m_wss;

			// stack descriptor to store error stack info
			CStackDescriptor m_sd;

			// list of objects to serialize on exception
			CList<CSerializable> m_listSerial;

			// spinlock protecting the list of serializable objects
			CSpinlockOS m_slockSerial;

			// minidump handler
			CMiniDumper *m_pmdr;

			// private copy ctor
			CErrorContext(const CErrorContext&);

		public:

			// ctor
			explicit
			CErrorContext(CMiniDumper *pmdr = NULL);
			
			// dtor
			virtual
			~CErrorContext();

			// reset context, clear out handled error
			virtual
			void Reset();
			
			// record error context
			virtual
			void Record(CException &exc, VA_LIST);

			// accessors
			virtual
			CException Exc() const
			{
				return m_exc;
			}
			
			virtual
			const WCHAR *WszMsg() const
			{
				return m_wsz;
			}
			
			CStackDescriptor *Psd()
			{
				return &m_sd;
			}

			CMiniDumper *Pmdr()
			{
				return m_pmdr;
			}

			// register minidump handler
			void Register
				(
				CMiniDumper *pmdr
				)
			{
				GPOS_ASSERT(NULL == m_pmdr);

				m_pmdr = pmdr;
			}

			// unregister minidump handler
			void Unregister
				(
#ifdef GPOS_DEBUG
				CMiniDumper *pmdr
#endif // GPOS_DEBUG
				)
			{
				GPOS_ASSERT(pmdr == m_pmdr);
				m_pmdr = NULL;
			}

			// register object to serialize
			void Register
				(
				CSerializable *pserial
				)
			{
				CAutoSpinlock asl(m_slockSerial);
				asl.Lock();

				m_listSerial.Append(pserial);
			}

			// unregister object to serialize
			void Unregister
				(
				CSerializable *pserial
				)
			{
				CAutoSpinlock asl(m_slockSerial);
				asl.Lock();

				m_listSerial.Remove(pserial);
			}

			// serialize registered objects
			void Serialize();

			// copy necessary info for error propagation
			virtual
			void CopyPropErrCtxt(const IErrorContext *perrctxt);

			// severity accessor
			virtual
			ULONG UlSev() const
			{
				return m_ulSev;
			}

			// set severity
			virtual
			void SetSev
				(
				ULONG ulSev
				)
			{
				m_ulSev = ulSev;
			}

			// print error stack trace
			virtual
			void AppendStackTrace()
			{
				m_wss.AppendFormat(GPOS_WSZ_LIT("\nStack trace:\n"));
				m_sd.AppendTrace(&m_wss);
			}

			// print errno message
			virtual
			void AppendErrnoMsg();

			virtual
			BOOL FPending() const
			{
				return m_fPending;
			}

			virtual
			BOOL FRethrow() const
			{
				return m_fRethrow;
			}

			virtual
			void SetRethrow()
			{
				m_fRethrow = true;
			}

	}; // class CErrorContext
}

#endif // !GPOS_CErrorContext_H

// EOF

