//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLPhysicalJoin.cpp
//
//	@doc:
//		Implementation of the base class for DXL physical join operators
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "naucrates/dxl/operators/CDXLPhysicalJoin.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalJoin::CDXLPhysicalJoin
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLPhysicalJoin::CDXLPhysicalJoin
	(
	IMemoryPool *pmp,
	EdxlJoinType edxljt
	)
	:
	CDXLPhysical(pmp),
	m_edxljt(edxljt)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalJoin::Edxltype
//
//	@doc:
//		Join type
//
//---------------------------------------------------------------------------
EdxlJoinType
CDXLPhysicalJoin::Edxltype() const
{
	return m_edxljt;
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLPhysicalJoin::PstrJoinTypeName
//
//	@doc:
//		Join type name
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLPhysicalJoin::PstrJoinTypeName() const
{
	return CDXLOperator::PstrJoinTypeName(m_edxljt);
}

// EOF
