//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSerializableQuery.cpp
//
//	@doc:
//		Serializable query object
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/task/CTask.h"

#include "naucrates/dxl/CDXLUtils.h"

#include "gpopt/minidump/CSerializableQuery.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CSerializableQuery::CSerializableQuery
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSerializableQuery::CSerializableQuery
	(
	const CDXLNode *pdxlnQuery, 
	const DrgPdxln *pdrgpdxlnQueryOutput,
	const DrgPdxln *pdrgpdxlnCTE
	)
	:
	CSerializable(),
	m_pdxlnQuery(pdxlnQuery),
	m_pdrgpdxlnQueryOutput(pdrgpdxlnQueryOutput),
	m_pdrgpdxlnCTE(pdrgpdxlnCTE),
	m_pstrQuery(NULL)
{
	GPOS_ASSERT(NULL != pdxlnQuery);
	GPOS_ASSERT(NULL != pdrgpdxlnQueryOutput);
}


//---------------------------------------------------------------------------
//	@function:
//		CSerializableQuery::~CSerializableQuery
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSerializableQuery::~CSerializableQuery()
{
	GPOS_DELETE(m_pstrQuery);
}


//---------------------------------------------------------------------------
//	@function:
//		CSerializableQuery::Serialize
//
//	@doc:
//		Serialize query to string
//
//---------------------------------------------------------------------------
void
CSerializableQuery::Serialize
	(
	IMemoryPool *pmp
	)
{
	GPOS_ASSERT(NULL == m_pstrQuery);

	m_pstrQuery =
		CDXLUtils::PstrSerializeQuery
				(
				pmp,
				m_pdxlnQuery,
				m_pdrgpdxlnQueryOutput,
				m_pdrgpdxlnCTE,
				false /*fSerializeHeaders*/,
				false /*fIndent*/
				);
}


//---------------------------------------------------------------------------
//	@function:
//		CSerializableQuery::UlpSerialize
//
//	@doc:
//		Serialize contents into provided buffer
//
//---------------------------------------------------------------------------
ULONG_PTR
CSerializableQuery::UlpSerialize
	(
	WCHAR *wszBuffer, 
	ULONG_PTR ulpAllocSize
	)
{
	ULONG_PTR ulpRequiredSpace = UlpRequiredSpace();
	
	GPOS_RTL_ASSERT(ulpAllocSize >= ulpRequiredSpace);
	
	if (0 < ulpRequiredSpace)
	{
		(void) clib::PvMemCpy
			(
			(BYTE *) wszBuffer,
			(BYTE *) m_pstrQuery->Wsz(),
			ulpRequiredSpace * GPOS_SIZEOF(WCHAR)
			);
	}
	
	return ulpRequiredSpace;
}

// EOF

