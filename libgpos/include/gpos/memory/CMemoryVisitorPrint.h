//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMemoryVisitorPrint.h
//
//	@doc:
//		Memory object visitor that prints debug information for all allocated
//		objects inside a memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_CMemoryVisitorPrint_H
#define GPOS_CMemoryVisitorPrint_H

#include "gpos/assert.h"
#include "gpos/types.h"
#include "gpos/utils.h"
#include "gpos/memory/IMemoryVisitor.h"

namespace gpos
{
    // specialization of memory object visitor that prints out
	// the debugging information to a stream
    class CMemoryVisitorPrint : public IMemoryVisitor
    {
        private:

            // call counter for the visit function
            ULLONG m_ullVisits;

            // stream used for writing debug information
            IOstream &m_os;

    		// private copy ctor
            CMemoryVisitorPrint(CMemoryVisitorPrint &);
			
        public:

            // ctor
            CMemoryVisitorPrint(IOstream &os);

            // dtor
            virtual
            ~CMemoryVisitorPrint();

            // output information about a memory allocation
            virtual
            void Visit
				(
				void *pvUserAddr,
				SIZE_T ulUserSize,
				void *pvTotalAddr,
				SIZE_T ulTotalSize,
				const CHAR * szAllocFilename,
				const ULONG ulAllocLine,
				ULLONG cAllocSeqNumber,
				CStackDescriptor *psd
				);

            // visit counter accessor
            ULLONG UllVisits() const
            {
            	return m_ullVisits;
            }
    };
}

#endif // GPOS_CMemoryVisitorPrint_H

// EOF

