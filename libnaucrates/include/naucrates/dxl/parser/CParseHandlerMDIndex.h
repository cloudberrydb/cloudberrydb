//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerMDIndex.h
//
//	@doc:
//		SAX parse handler class for parsing an MD index
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDIndex_H
#define GPDXL_CParseHandlerMDIndex_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDIndex
	//
	//	@doc:
	//		Parse handler class for parsing an MD index
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDIndex : public CParseHandlerMetadataObject
	{
		private:
		
			// mdid of the index
			IMDId *m_pmdid;
			
			// name of the index
			CMDName *m_pmdname;

			// mdid of the indexed relation
			IMDId *m_pmdidRel;
			
			// is the index clustered
			BOOL m_fClustered;

			// index type
			IMDIndex::EmdindexType m_emdindt;
			
			// type id of index items
			// for instance, for bitmap indexes, this is the type id of the bitmap
			IMDId *m_pmdidItemType;

			// index keys
			DrgPul *m_pdrgpulKeyCols;

			// included columns
			DrgPul *m_pdrgpulIncludedCols;
			
			// index part constraint
			CMDPartConstraintGPDB *m_ppartcnstr;
			
			// levels that include default partitions
			DrgPul *m_pdrgpulDefaultParts;
			
			// is constraint unbounded
			BOOL m_fPartConstraintUnbounded; 
			
			// private copy ctor
			CParseHandlerMDIndex(const CParseHandlerMDIndex&);

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
			CParseHandlerMDIndex
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerMDIndex_H

// EOF
