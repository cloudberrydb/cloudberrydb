//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008-2010 Greenplum Inc.
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMemoryVisitor.h
//
//	@doc:
//      Interface for applying a common operation to all allocated objects
//		inside a memory pool.
//
//	@owner:
//
//	@test:
//
//---------------------------------------------------------------------------
#ifndef GPOS_IMemoryVisitor_H
#define GPOS_IMemoryVisitor_H

#include "gpos/assert.h"
#include "gpos/types.h"

namespace gpos
{
	// prototypes
	class CStackDescriptor;

	// wrapper for common operation on allocated memory;
	// called by memory pools when a walk of the memory is requested;
    class IMemoryVisitor
    {

    	private:

            // private copy ctor
            IMemoryVisitor(IMemoryVisitor &);

        public:

            // ctor
            IMemoryVisitor()
            {}

            // dtor
            virtual
            ~IMemoryVisitor()
            {}

            // executed operation during a walk of objects;
            // file name may be NULL (when debugging is not enabled);
            // line number will be zero in that case;
            // sequence number is a constant in case allocation sequencing is not supported;
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
                ) = 0;
    };
}

#endif // GPOS_IMemoryVisitor_H

// EOF

