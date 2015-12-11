//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerXform.h
//
//	@doc:
//		SAX parse handler class for parsing xform nodes.
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerXform_H
#define GPDXL_CParseHandlerXform_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

// forward declarations
namespace gpopt
{
	class CXform;
}

namespace gpdxl
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerXform
	//
	//	@doc:
	//		Parse handler for parsing xform
	//
	//---------------------------------------------------------------------------
	class CParseHandlerXform : public CParseHandlerBase
	{

		private:

			// xform referred to by XML node
			CXform *m_pxform;

			// private copy ctor
			CParseHandlerXform(const CParseHandlerXform&);

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
			CParseHandlerXform
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual
			~CParseHandlerXform();

			// returns the root of constructed DXL plan
			CXform *Pxform()
			{
				return m_pxform;
			}

			EDxlParseHandlerType Edxlphtype() const
			{
				return EdxlphSearchStrategy;
			}

	};
}

#endif // !GPDXL_CParseHandlerXform_H

// EOF
