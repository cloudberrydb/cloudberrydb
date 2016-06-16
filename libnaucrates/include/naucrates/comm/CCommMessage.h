//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCommMessage.h
//
//	@doc:
//		Descriptor of message exchanged between OPT and QD processes
//---------------------------------------------------------------------------
#ifndef GPNAUCRATES_CCommMessage_H
#define GPNAUCRATES_CCommMessage_H

#include "gpos/base.h"

#define GPNAUCRATES_COMM_MESSAGE_BUFFER	(32)

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CCommMessage
	//
	//	@doc:
	//		Descriptor of message exchanged between OPT and QD processes
	//
	//---------------------------------------------------------------------------
	class CCommMessage
	{
		public:

			// message type
			enum ECommMessageType
			{
				EcmtOptRequest,
				EcmtOptResponse,
				EcmtMDRequest,
				EcmtMDResponse,
				EcmtLog,
				EcmtSentinel
			};

		private:

			// message type
			const ECommMessageType m_cmt;

			// message, formatted as wide character string
			const WCHAR *m_wsz;

			// message-related info
			const ULLONG m_ullInfo;

			// private copy ctor
			CCommMessage(const CCommMessage&);

		public:

			// ctor
			CCommMessage
				(
				ECommMessageType cmt,
				const WCHAR *wsz,
				ULLONG ullData
				)
				:
				m_cmt(cmt),
				m_wsz(wsz),
				m_ullInfo(ullData)
			{
				GPOS_ASSERT(NULL != wsz);
			}


			// dtor
			~CCommMessage()
			{}

			// message accessor
			const WCHAR *Wsz() const
			{
				return m_wsz;
			}

			// type accessor
			ECommMessageType Ecmt() const
			{
				return m_cmt;
			}

			// info accessor
			ULLONG UllInfo() const
			{
				return m_ullInfo;
			}

			// check if message type is valid
			static
			BOOL FCheckType
				(
				ULONG ulType
				)
			{
				return EcmtSentinel > ulType;
			}

	}; // class CCommMessage
}

#endif // !GPNAUCRATES_CCommMessage_H


// EOF
