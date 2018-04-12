//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadataColumn.h
//
//	@doc:
//		SAX parse handler class for column metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMetadataColumn_H
#define GPDXL_CParseHandlerMetadataColumn_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/md/CMDColumn.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMetadataColumn
	//
	//	@doc:
	//		Parse handler for column metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMetadataColumn : public CParseHandlerBase
	{
		private:
			// the metadata column
			CMDColumn *m_pmdcol;
			
			// column name
			CMDName *m_pmdname;
			
			// attribute number
			INT m_iAttNo;
			
			// attribute type oid
			IMDId *m_pmdidType;

			INT m_iTypeModifier;

			OID m_oidCollation;

			// are nulls allowed for this column
			BOOL m_fNullable;
			
			// is column dropped
			BOOL m_fDropped;
			
			// default value expression if one exists
			CDXLNode *m_pdxlnDefaultValue;

			// width of the column
			ULONG m_ulWidth;
			
			// private copy ctor
			CParseHandlerMetadataColumn(const CParseHandlerMetadataColumn &);
			
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
			CParseHandlerMetadataColumn
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			~CParseHandlerMetadataColumn();
			
			CMDColumn *Pmdcol();
			
	};
}

#endif // !GPDXL_CParseHandlerMetadataColumn_H

// EOF
