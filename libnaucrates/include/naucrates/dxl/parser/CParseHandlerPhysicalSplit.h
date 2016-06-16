//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerPhysicalSplit.h
//
//	@doc:
//		Parse handler for parsing a physical split operator
//
//---------------------------------------------------------------------------
#ifndef GPDXL_CParseHandlerPhysicalSplit_H
#define GPDXL_CParseHandlerPhysicalSplit_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"


namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerPhysicalSplit
	//
	//	@doc:
	//		Parse handler for parsing a physical split operator
	//
	//---------------------------------------------------------------------------
	class CParseHandlerPhysicalSplit : public CParseHandlerPhysicalOp
	{
		private:

			// deletion col ids
			DrgPul *m_pdrgpulDelete;

			// insertion col ids
			DrgPul *m_pdrgpulInsert;

			// action column id
			ULONG m_ulAction;

			// ctid column id
			ULONG m_ulCtid;

			// segmentId column id
			ULONG m_ulSegmentId;

			// does update preserve oids
			BOOL m_fPreserveOids;
			
			// tuple oid column id
			ULONG m_ulTupleOidColId;
			
			// private copy ctor
			CParseHandlerPhysicalSplit(const CParseHandlerPhysicalSplit &);

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
			CParseHandlerPhysicalSplit
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerPhysicalSplit_H

// EOF
