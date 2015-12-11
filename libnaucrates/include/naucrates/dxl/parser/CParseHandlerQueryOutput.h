//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerQueryOutput.h
//
//	@doc:
//		SAX parse handler class for parsing the list of output column references
//		in the DXL representation of the query.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerQueryOutput_H
#define GPDXL_CParseHandlerQueryOutput_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerQueryOutput
	//
	//	@doc:
	//		Parse handler class for parsing the list of output column references
	//		in a DXL representation of the query
	//
	//---------------------------------------------------------------------------
	class CParseHandlerQueryOutput : public CParseHandlerBase
	{
		private:

			// list of scalar ident nodes representing the query output
			DrgPdxln *m_pdrgpdxln;

			// private copy ctor
			CParseHandlerQueryOutput(const CParseHandlerQueryOutput&);

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
			// ctor/dtor
			CParseHandlerQueryOutput
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			virtual
			~CParseHandlerQueryOutput();


			// return the list of output scalar ident nodes
			DrgPdxln *PdrgpdxlnOutputCols();
	};
}

#endif // !GPDXL_CParseHandlerQueryOutput_H

// EOF
