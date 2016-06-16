//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBCheckConstraint.h
//
//	@doc:
//		SAX parse handler class for parsing an MD check constraint
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDGPDBCheckConstraint_H
#define GPDXL_CParseHandlerMDGPDBCheckConstraint_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDGPDBCheckConstraint
	//
	//	@doc:
	//		Parse handler class for parsing an MD check constraint
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDGPDBCheckConstraint : public CParseHandlerMetadataObject
	{
		private:

			// mdid of the check constraint
			IMDId *m_pmdid;

			// name of the check constraint
			CMDName *m_pmdname;

			// mdid of the relation
			IMDId *m_pmdidRel;

			// private copy ctor
			CParseHandlerMDGPDBCheckConstraint(const CParseHandlerMDGPDBCheckConstraint&);

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
			CParseHandlerMDGPDBCheckConstraint
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerMDGPDBCheckConstraint_H

// EOF
