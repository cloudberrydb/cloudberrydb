//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadataColumns.h
//
//	@doc:
//		SAX parse handler class for columns metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMetadataColumns_H
#define GPDXL_CParseHandlerMetadataColumns_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/md/CMDColumn.h"
#include "naucrates/md/CMDRelationGPDB.h"


namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMetadataColumns
	//
	//	@doc:
	//		Parse handler for relation columns
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMetadataColumns : public CParseHandlerBase
	{
		private:
			// list of columns
			DrgPmdcol *m_pdrgpmdcol;
		
			// private copy ctor
			CParseHandlerMetadataColumns(const CParseHandlerMetadataColumns &);
			
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
			CParseHandlerMetadataColumns
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			~CParseHandlerMetadataColumns();

			
			// returns the constructed columns list
			DrgPmdcol *Pdrgpmdcol();
			
	};
}

#endif // !GPDXL_CParseHandlerMetadataColumns_H

// EOF
