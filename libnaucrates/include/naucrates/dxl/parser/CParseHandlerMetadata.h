//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadata.h
//
//	@doc:
//		SAX parse handler class for parsing metadata from a DXL document.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMetadata_H
#define GPDXL_CParseHandlerMetadata_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "naucrates/md/IMDCacheObject.h"

#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE
	
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMetadata
	//
	//	@doc:
	//		Parse handler for metadata.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMetadata : public CParseHandlerBase
	{
		private:
			
			// list of parsed metadata objects
			DrgPimdobj *m_pdrgpmdobj;
			
			// list of parsed mdids
			DrgPmdid *m_pdrgpmdid;

			// list of parsed metatadata source system ids
			DrgPsysid *m_pdrgpsysid;

			// private copy ctor
			CParseHandlerMetadata(const CParseHandlerMetadata&);
			
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
			
			// parse an array of system ids from the XML attributes
			DrgPsysid *PdrgpsysidParse
						(	
						const Attributes &attr,
						Edxltoken edxltokenAttr,
						Edxltoken edxltokenElement
						);

			
		public:
			// ctor
			CParseHandlerMetadata(IMemoryPool *pmp, CParseHandlerManager *pphm, CParseHandlerBase *pphRoot);
			
			// dtor
			virtual
			~CParseHandlerMetadata();
			
			// parse hander type
			virtual
			EDxlParseHandlerType Edxlphtype() const;
			
			// return the list of parsed metadata objects
			DrgPimdobj *Pdrgpmdobj();
			
			// return the list of parsed mdids
			DrgPmdid *Pdrgpmdid();
			
			// return the list of parsed system ids
			DrgPsysid *Pdrgpsysid();

	};
}

#endif // !GPDXL_CParseHandlerMetadata_H

// EOF
