//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMetadataObject.cpp
//
//	@doc:
//		Implementation of the base SAX parse handler class for parsing metadata objects.
//---------------------------------------------------------------------------

#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"

using namespace gpdxl;


XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataObject::CParseHandlerMetadataObject
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CParseHandlerMetadataObject::CParseHandlerMetadataObject
	(
	IMemoryPool *pmp, 
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pimdobj(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataObject::~CParseHandlerMetadataObject
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CParseHandlerMetadataObject::~CParseHandlerMetadataObject()
{
	CRefCount::SafeRelease(m_pimdobj);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerMetadataObject::Pimdobj
//
//	@doc:
//		Returns the constructed metadata object.
//
//---------------------------------------------------------------------------
IMDCacheObject *
CParseHandlerMetadataObject::Pimdobj() const
{
	return m_pimdobj;
}




// EOF

