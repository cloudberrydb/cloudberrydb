//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMiniDumper.h
//
//	@doc:
//		Interface for minidump handler;
//---------------------------------------------------------------------------
#ifndef GPOS_CMiniDumper_H
#define GPOS_CMiniDumper_H

#include "gpos/base.h"

#define GPOS_MINIDUMPER_DEFAULT_CAPACITY	(4 * 1024 * 1024)


namespace gpos
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMiniDumper
	//
	//	@doc:
	//		Interface for minidump handler;
	//
	//---------------------------------------------------------------------------
	class CMiniDumper : CStackObject
	{
		private:

			// memory pool
			IMemoryPool *m_pmp;

			// pre-allocated buffer;
			// used to serialize objects
			WCHAR *m_wszBuffer;

			// used buffer size
			volatile ULONG_PTR m_ulpUsed;

			// buffer capacity
			const ULONG_PTR m_ulpCapacity;

			// flag indicating if handler is initialized
			BOOL m_fInit;

			// flag indicating if handler is finalized
			BOOL m_fFinal;

			// flag indicating if buffer has been converted to multi-byte format
			BOOL m_fConverted;

			// private copy ctor
			CMiniDumper(const CMiniDumper&);

		public:

			// ctor
			CMiniDumper
				(
				IMemoryPool *pmp,
				ULONG_PTR ulpCapacity = GPOS_MINIDUMPER_DEFAULT_CAPACITY
				);

			// dtor
			virtual
			~CMiniDumper();

			// initialize
			void Init();

			// finalize
			void Finalize();

			// reserve space for minidump entry
			WCHAR *WszReserve(ULONG_PTR ulpSize);

			// write to output stream
			IOstream &OsPrint(IOstream &os) const;

			// convert to multi-byte string
			ULONG_PTR UlpConvertToMultiByte();

			// raw accessor to buffer
			const BYTE *Pb() const
			{
				return reinterpret_cast<const BYTE*>(m_wszBuffer);
			}

			// serialize minidump header
			virtual
			void SerializeHeader() = 0;

			// serialize minidump footer
			virtual
			void SerializeFooter() = 0;

			// size to reserve for entry header
			virtual
			ULONG_PTR UlpRequiredSpaceEntryHeader() = 0;

			// size to reserve for entry header
			virtual
			ULONG_PTR UlpRequiredSpaceEntryFooter() = 0;

			// serialize entry header
			virtual
			ULONG_PTR UlpSerializeEntryHeader(WCHAR *wszEntry, ULONG_PTR ulpAllocSize) = 0;

			// serialize entry footer
			virtual
			ULONG_PTR UlpSerializeEntryFooter(WCHAR *wszEntry, ULONG_PTR ulpAllocSize) = 0;

	}; // class CMiniDumper

	// shorthand for printing
	inline
	IOstream &operator << (IOstream &os, const CMiniDumper &mdr)
	{
		return mdr.OsPrint(os);
	}
}

#endif // !GPOS_CMiniDumper_H

// EOF

