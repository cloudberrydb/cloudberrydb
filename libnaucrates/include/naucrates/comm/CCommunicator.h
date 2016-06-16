//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CCommunicator.h
//
//	@doc:
//		Communication handler for message exchange between OPT and QD
//---------------------------------------------------------------------------
#ifndef GPOPT_CCommunicator_H
#define GPOPT_CCommunicator_H

#include "gpos/base.h"
#include "gpos/net/CSocket.h"
#include "gpos/sync/CMutex.h"

#include "naucrates/comm/CCommMessage.h"

namespace gpnaucrates
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CCommunicator
	//
	//	@doc:
	//		Communication handler for message exchange between OPT and QD
	//
	//---------------------------------------------------------------------------
	class CCommunicator
	{
		private:

			//---------------------------------------------------------------------------
			//	@class:
			//		SMessageHeader
			//
			//	@doc:
			//		Message header; sent separately from message body
			//
			//---------------------------------------------------------------------------
			struct SMessageHeader
			{
				// id
				ULLONG m_ullId;

				// message type
				ULONG m_ulType;

				// message size
				ULONG m_ulSize;

				// message info
				ULLONG m_ullInfo;

				// sequence used to assign ids;
				// updated under the protection of communicator's mutex;
				static
				ULLONG m_ullSequence;

				// ctor
				SMessageHeader()
				{}

				// ctor
				SMessageHeader
					(
					CCommMessage *pmsg
					)
					:
					m_ullId(m_ullSequence++),
					m_ulType(pmsg->Ecmt()),
					m_ulSize(GPOS_WSZ_LENGTH(pmsg->Wsz()) + 1),
					m_ullInfo(pmsg->UllInfo())
				{}
			};

			// memory pool
			IMemoryPool *m_pmp;

			// socket used to send and receive data
			CSocket *m_psocket;

			// mutex protecting socket operations
			CMutex m_mutex;

			// send message - unsynchronized
			void Send(CCommMessage *pmsg);

			// receive message - unsynchronized
			CCommMessage *PmsgReceive();

			// private copy ctor
			CCommunicator(const CCommunicator&);

		public:

			// ctor
			CCommunicator
				(
				IMemoryPool *pmp,
				CSocket *psocket
				)
				:
				m_pmp(pmp),
				m_psocket(psocket)
			{
				GPOS_ASSERT(NULL != pmp);
				GPOS_ASSERT(NULL != psocket);
			}

			// dtor
			~CCommunicator()
			{}

			// send message
			void SendMsg(CCommMessage *pmsg);

			// receive message
			CCommMessage *PmsgReceiveMsg();

			// send request and block until response is received
			CCommMessage *PmsgSendRequest(CCommMessage *pmsg);

			// check socket condition
			BOOL FCheck() const
			{
				return m_psocket->FCheck();
			}

	}; // class CCommunicator
}

#endif // !GPOPT_CCommunicator_H


// EOF
