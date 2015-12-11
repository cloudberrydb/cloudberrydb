//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CParseHandlerPartitionSelector.h
//
//	@doc:
//		SAX parse handler class for parsing a partition selector
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarPartitionSelector_H
#define GPDXL_CParseHandlerScalarPartitionSelector_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPartitionSelector
	//
	//	@doc:
	//		Parse handler class for parsing a partition selector
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPartitionSelector : public CParseHandlerPhysicalOp
	{
		private:

			// table id
			IMDId *m_pmdidRel;

			// number of partitioning levels
			ULONG m_ulLevels;

			// scan id
			ULONG m_ulScanId;

			// private copy ctor
			CParseHandlerPartitionSelector(const CParseHandlerPartitionSelector&);

			// process the start of an element
			void StartElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
				);

			// process the end of an element
			void EndElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
				);

		public:
			// ctor
			CParseHandlerPartitionSelector
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerScalarPartitionSelector_H

// EOF
