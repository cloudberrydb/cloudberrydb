//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CDXLSpoolInfo.cpp
//
//	@doc:
//		Implementation of DXL sorting columns for sort and motion operator nodes
//---------------------------------------------------------------------------

#include "naucrates/dxl/operators/CDXLSpoolInfo.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"


using namespace gpos;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::CDXLSpoolInfo
//
//	@doc:
//		Constructor
//
//---------------------------------------------------------------------------
CDXLSpoolInfo::CDXLSpoolInfo
	(
	ULONG ulSpoolId,
	Edxlspooltype edxlspstype,
	BOOL fMultiSlice,
	INT iExecutorSlice
	)
	:
	m_ulSpoolId(ulSpoolId),
	m_edxlsptype(edxlspstype),
	m_fMultiSlice(fMultiSlice),
	m_iExecutorSlice(iExecutorSlice)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::UlSpoolId
//
//	@doc:
//		Spool id
//
//---------------------------------------------------------------------------
ULONG
CDXLSpoolInfo::UlSpoolId() const
{
	return m_ulSpoolId;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::Edxlsptype
//
//	@doc:
//		Type of the underlying operator (Materialize or Sort)
//
//---------------------------------------------------------------------------
Edxlspooltype
CDXLSpoolInfo::Edxlsptype() const
{
	return m_edxlsptype;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::FMultiSlice
//
//	@doc:
//		Is the spool used across slices
//
//---------------------------------------------------------------------------
BOOL
CDXLSpoolInfo::FMultiSlice() const
{
	return m_fMultiSlice;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::IExecutorSlice
//
//	@doc:
//		Id of slice executing the underlying operation
//
//---------------------------------------------------------------------------
INT
CDXLSpoolInfo::IExecutorSlice() const
{
	return m_iExecutorSlice;
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::PstrSpoolType
//
//	@doc:
//		Id of slice executing the underlying operation
//
//---------------------------------------------------------------------------
const CWStringConst *
CDXLSpoolInfo::PstrSpoolType() const
{
	GPOS_ASSERT(EdxlspoolMaterialize == m_edxlsptype || EdxlspoolSort == m_edxlsptype);
	
	switch (m_edxlsptype)
	{
		case EdxlspoolMaterialize:
			return CDXLTokens::PstrToken(EdxltokenSpoolMaterialize);
		case EdxlspoolSort:
			return CDXLTokens::PstrToken(EdxltokenSpoolSort);
		default:
			return CDXLTokens::PstrToken(EdxltokenUnknown);
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CDXLSpoolInfo::SerializeToDXL
//
//	@doc:
//		Serialize spooling info in DXL format
//
//---------------------------------------------------------------------------
void
CDXLSpoolInfo::SerializeToDXL
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSpoolId), m_ulSpoolId);
	
	const CWStringConst *pstrSpoolType = PstrSpoolType();
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSpoolType), pstrSpoolType);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenSpoolMultiSlice), m_fMultiSlice);

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenExecutorSliceId), m_iExecutorSlice);
	

}




// EOF
