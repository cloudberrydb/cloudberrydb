//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerPlan.h
//
//	@doc:
//		SAX parse handler class for converting physical plans from a DXL document
//		into a DXL tree.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerPlan_H
#define GPDXL_CParseHandlerPlan_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	// fwd decl
	class CDXLDirectDispatchInfo;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPlan
	//
	//	@doc:
	//		Parse handler for converting physical plans from a DXL document
	//		into a DXL tree.
	//---------------------------------------------------------------------------
	class CParseHandlerPlan : public CParseHandlerBase 
	{
		private:

			// plan id
			ULLONG m_ullId;

			// size of plan space
			ULLONG m_ullSpaceSize;

			// the root of the parsed DXL tree constructed by the parse handler
			CDXLNode *m_pdxln;
			
			// direct dispatch info spec
			CDXLDirectDispatchInfo *m_pdxlddinfo;
			
			// private ctor 
			CParseHandlerPlan(const CParseHandlerPlan&);
						
			// process the end of an element
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
			CParseHandlerPlan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			virtual
			~CParseHandlerPlan();
			
			// returns the root of constructed DXL plan
			CDXLNode *Pdxln();
			
			// return plan id
			ULLONG UllId() const
			{
				return m_ullId;
			}

			// return size of plan space
			ULLONG UllSpaceSize() const
			{
				return m_ullSpaceSize;
			}

			EDxlParseHandlerType Edxlphtype() const;
			
	};
}

#endif // !GPDXL_CParseHandlerPlan_H

// EOF
