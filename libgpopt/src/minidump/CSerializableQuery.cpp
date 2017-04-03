//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSerializableQuery.cpp
//
//	@doc:
//		Serializable query object
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
	const DrgPdxln *pdrgpdxlnCTE,
	IMemoryPool *pmp
	)
	:
	CSerializable(),
	m_pdxlnQuery(pdxlnQuery),
	m_pdrgpdxlnQueryOutput(pdrgpdxlnQueryOutput),
	m_pdrgpdxlnCTE(pdrgpdxlnCTE),
	m_pmp(pmp)
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
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableQuery::Serialize
//
//	@doc:
//		Serialize contents into provided stream
//
//---------------------------------------------------------------------------
void
CSerializableQuery::Serialize
	(
	COstream &oos
	)
{
	CDXLUtils::SerializeQuery
			(
			m_pmp,
			oos,
			m_pdxlnQuery,
			m_pdrgpdxlnQueryOutput,
			m_pdrgpdxlnCTE,
			false /*fSerializeHeaders*/,
			false /*fIndent*/
			);
}

// EOF

