//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CAutoFileDescriptor.h
//
//	@doc:
//		Auto object holding file descriptor
//---------------------------------------------------------------------------
#ifndef GPOS_CAutoFileDescriptor_H
#define GPOS_CAutoFileDescriptor_H

#include "gpos/base.h"
#include "gpos/io/ioutils.h"
#include "gpos/io/CFileDescriptor.h"

namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CAutoFileDescriptor
	//
	//	@doc:
	//		Auto object holding file descriptor; calls close() on dtor;
	//
	//---------------------------------------------------------------------------

	class CAutoFileDescriptor
	{
		private:

			// file descriptor
			INT m_iFileDescr;

			// no copy ctor
			CAutoFileDescriptor(const CAutoFileDescriptor&);

		public:

			// ctor
			explicit
			CAutoFileDescriptor
				(
				INT iFileDescr
				)
				:
				m_iFileDescr(iFileDescr)
			{
				GPOS_ASSERT(GPOS_FILE_DESCR_INVALID != iFileDescr);
			}

			// dtor
			~CAutoFileDescriptor()
			{
				if (GPOS_FILE_DESCR_INVALID != m_iFileDescr)
				{
					(void) ioutils::IClose(m_iFileDescr);
				}
			};

			// file desciptor accessor
			INT IFileDescr() const
			{
				GPOS_ASSERT(GPOS_FILE_DESCR_INVALID != m_iFileDescr);

				return m_iFileDescr;
			}

			// detach from file descriptor
			void Detach()
			{
				m_iFileDescr = GPOS_FILE_DESCR_INVALID;
			}

	};	// class CAutoFileDescriptor
}

#endif // !GPOS_CAutoFileDescriptor_H

// EOF

