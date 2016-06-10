//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadataIdList.h
//
//	@doc:
//		SAX parse handler class for parsing a list of metadata identifiers
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMetadataIdList_H
#define GPDXL_CParseHandlerMetadataIdList_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMetadataIdList
	//
	//	@doc:
	//		SAX parse handler class for parsing a list of metadata identifiers
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMetadataIdList : public CParseHandlerBase
	{
		private:
			// list of metadata identifiers
			DrgPmdid *m_pdrgpmdid;

			
			// private copy ctor
			CParseHandlerMetadataIdList(const CParseHandlerMetadataIdList &);
			
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
			
			// is this a supported element of a metadata list
			BOOL FSupportedElem(const XMLCh * const xmlsz);
			
			// is this a supported metadata list type
			BOOL FSupportedListType(const XMLCh * const xmlsz);
			
		public:
			// ctor/dtor
			CParseHandlerMetadataIdList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			virtual
			~CParseHandlerMetadataIdList();
			
			// return the constructed list of metadata identifiers
			DrgPmdid *Pdrgpmdid();
			
	};
}

#endif // !GPDXL_CParseHandlerMetadataIdList_H

// EOF
