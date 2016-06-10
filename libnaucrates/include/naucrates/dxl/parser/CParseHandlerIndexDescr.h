//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerIndexDescr.h
//
//	@doc:
//		SAX parse handler class for parsing the index descriptor portions
//		of index scan operator nodes
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerIndexDescriptor_H
#define GPDXL_CParseHandlerIndexDescriptor_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "naucrates/dxl/operators/CDXLIndexDescr.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerIndexDescr
	//
	//	@doc:
	//		Parse handler for parsing an index descriptor
	//
	//---------------------------------------------------------------------------
	class CParseHandlerIndexDescr : public CParseHandlerBase
	{
		private:

			// the index descriptor to construct
			CDXLIndexDescr *m_pdxlid;

			// private copy ctor
			CParseHandlerIndexDescr(const CParseHandlerIndexDescr &);

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
			CParseHandlerIndexDescr
				(
				IMemoryPool *,
				CParseHandlerManager *,
				CParseHandlerBase *
				);

			//dtor
			~CParseHandlerIndexDescr();

			CDXLIndexDescr *Pdxlid();

	};
}

#endif // !GPDXL_CParseHandlerIndexDescriptor_H

// EOF
