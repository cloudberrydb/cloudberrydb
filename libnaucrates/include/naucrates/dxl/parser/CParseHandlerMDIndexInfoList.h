//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software, Inc.
//
//	@filename:
//		CParseHandlerMDIndexInfoList.h
//
//	@doc:
//		SAX parse handler class for parsing an indexinfo list
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDIndexInfoList_H
#define GPDXL_CParseHandlerMDIndexInfoList_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/IMDRelation.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	// parse handler class for parsing indexinfo list
	class CParseHandlerMDIndexInfoList : public CParseHandlerBase
	{
		private:
			// list of indexinfo
			DrgPmdIndexInfo *m_pdrgpmdIndexInfo;

			// private copy ctor
			CParseHandlerMDIndexInfoList(const CParseHandlerMDIndexInfoList&);

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
			CParseHandlerMDIndexInfoList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual
			~CParseHandlerMDIndexInfoList();

			// returns array of indexinfo
			DrgPmdIndexInfo *PdrgpmdIndexInfo();
	};
}

#endif // !GPDXL_CParseHandlerMDIndexInfoList_H

// EOF
