//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadataObject.h
//
//	@doc:
//		Base SAX parse handler class for parsing metadata objects.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMetadataObject_H
#define GPDXL_CParseHandlerMetadataObject_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/md/IMDCacheObject.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMetadataObject
	//
	//	@doc:
	//		Base parse handler class for metadata objects
	//
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMetadataObject : public CParseHandlerBase 
	{
		private:

			// private copy ctor
			CParseHandlerMetadataObject(const CParseHandlerMetadataObject&);
			
			
		protected:
			// the metadata object constructed by the parse handler
			IMDCacheObject *m_pimdobj;
						
		public:
			// ctor/dtor
			CParseHandlerMetadataObject
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			virtual
			~CParseHandlerMetadataObject();
			
			// returns constructed metadata object
			IMDCacheObject *Pimdobj() const;	
	};
}

#endif // !GPDXL_CParseHandlerMetadataObject_H

// EOF
