//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerBase.h
//
//	@doc:
//		Base SAX parse handler class for DXL documents
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerBase_H
#define GPDXL_CParseHandlerBase_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/dxlops.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/CDXLUtils.h"

#include <xercesc/sax2/DefaultHandler.hpp>

namespace gpdxl
{
	using namespace gpos;

	// fwd decl
	class CParseHandlerManager;
	class CParseHandlerBase;
	
	// dynamic arrays of parse handlers
	typedef CDynamicPtrArray<CParseHandlerBase, CleanupDelete> DrgPph;
	
	XERCES_CPP_NAMESPACE_USE

	// DXL parse handler type. Currently we only annotate the top-level parse handlers
	// for the top-level DXL elements such as Plan, Query and Metadata
	enum EDxlParseHandlerType
	{
		EdxlphOptConfig,
		EdxlphEnumeratorConfig,
		EdxlphStatisticsConfig,
		EdxlphCTEConfig,
		EdxlphHint,
		EdxlphWindowOids,
		EdxlphTraceFlags,
		EdxlphPlan,
		EdxlphQuery,
		EdxlphMetadata,
		EdxlphMetadataRequest,
		EdxlphStatistics,
		EdxlphSearchStrategy,
		EdxlphCostParams,
		EdxlphCostParam,
		EdxlphScalarExpr,
		EdxlphOther
	};
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerBase
	//
	//	@doc:
	//		Base SAX parse handler class for parsing DXL documents.
	//		Implements Xerces required interface for SAX parse handlers.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerBase : public DefaultHandler 
	{

		private:
		
			// private copy ctor
			CParseHandlerBase(const CParseHandlerBase&); 
			
			// array of parse handlers for child elements
			DrgPph *m_pdrgpph;
			
		protected:
			// memory pool to create DXL objects in
			IMemoryPool *m_pmp;
			
			// manager for transitions between parse handlers
			CParseHandlerManager *m_pphm;

			// add child parse handler
			inline
			void Append
				(
				CParseHandlerBase *pph
				)
			{
				GPOS_ASSERT(NULL != pph);
				m_pdrgpph->Append(pph);
			};

			// number of children
			inline
			ULONG UlLength() const
			{
				return m_pdrgpph->UlLength();
			}

			// shorthand to access children
			inline
			CParseHandlerBase *operator []
				(
				ULONG ulPos
				)
				const
			{
				return (*m_pdrgpph)[ulPos];
			};
			
			// parse handler for root element
			CParseHandlerBase *m_pphRoot;
			
			// process the start of an element
			virtual 
			void StartElement
				(
				const XMLCh* const xmlszUri, 		// URI of element's namespace
				const XMLCh* const xmlszLocalname,	// local part of element's name
				const XMLCh* const xmlszQname,		// element's qname
				const Attributes& attr				// element's attributes
				) = 0;
				
			// process the end of an element
			virtual 
			void EndElement
				(
				const XMLCh* const xmlszUri, 		// URI of element's namespace
				const XMLCh* const xmlszLocalname,	// local part of element's name
				const XMLCh* const xmlszQname		// element's qname
				) = 0;

		public:
			// ctor
			CParseHandlerBase(IMemoryPool *pmp, CParseHandlerManager *pphm, CParseHandlerBase *pphRoot);
			
			//dtor
			virtual
			~CParseHandlerBase();
			
			virtual 
			EDxlParseHandlerType Edxlphtype() const;

			// replaces a parse handler in the parse handler array with a new one
			void ReplaceParseHandler(CParseHandlerBase *pphOld, CParseHandlerBase *pphNew);
			
			// Xerces parse handler interface method to eceive notification of the beginning of an element.
			void startElement
				(
				const XMLCh* const xmlszUri, 		// URI of element's namespace
				const XMLCh* const xmlszLocalname,	// local part of element's name
				const XMLCh* const xmlszQname,		// element's qname
				const Attributes& attr				// element's attributes
				);

			// Xerces parse handler interface method to eceive notification of the end of an element.
			void endElement
				(
				const XMLCh* const xmlszUri, 		// URI of element's namespace
				const XMLCh* const xmlszLocalname,	// local part of element's name
				const XMLCh* const xmlszQname		// element's qname
				);
			
			// process a parsing error
			void error(const SAXParseException&);
	};
}

#endif // !GPDXL_CParseHandlerBase_H

// EOF
