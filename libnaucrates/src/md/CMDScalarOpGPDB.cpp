//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDScalarOpGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific scalar ops
//		in the MD cache
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDScalarOpGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpdxl;
using namespace gpmd;

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::CMDScalarOpGPDB
//
//	@doc:
//		Constructs a metadata scalar op
//
//---------------------------------------------------------------------------
CMDScalarOpGPDB::CMDScalarOpGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	IMDId *pmdidTypeLeft,
	IMDId *pmdidTypeRight,
	IMDId *pmdidTypeResult,
	IMDId *pmdidFunc,
	IMDId *pmdidOpCommute,
	IMDId *pmdidOpInverse,
	IMDType::ECmpType ecmpt,
	BOOL fReturnsNullOnNullInput,
	DrgPmdid *pdrgpmdidOpClasses
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_pmdidTypeLeft(pmdidTypeLeft),
	m_pmdidTypeRight(pmdidTypeRight),
	m_pmdidTypeResult(pmdidTypeResult),
	m_pmdidFunc(pmdidFunc),
	m_pmdidOpCommute(pmdidOpCommute),
	m_pmdidOpInverse(pmdidOpInverse),
	m_ecmpt(ecmpt),
	m_fReturnsNullOnNullInput(fReturnsNullOnNullInput),
	m_pdrgpmdidOpClasses(pdrgpmdidOpClasses)
{
	GPOS_ASSERT(NULL != pdrgpmdidOpClasses);
	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::~CMDScalarOpGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDScalarOpGPDB::~CMDScalarOpGPDB()
{
	m_pmdid->Release();
	m_pmdidTypeResult->Release();
	m_pmdidFunc->Release();	

	CRefCount::SafeRelease(m_pmdidTypeLeft);
	CRefCount::SafeRelease(m_pmdidTypeRight);
	CRefCount::SafeRelease(m_pmdidOpCommute);
	CRefCount::SafeRelease(m_pmdidOpInverse);
	
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pdrgpmdidOpClasses->Release();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::Pmdid
//
//	@doc:
//		Operator id
//
//---------------------------------------------------------------------------
IMDId *
CMDScalarOpGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::Mdname
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
CMDName
CMDScalarOpGPDB::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::PmdidTypeLeft
//
//	@doc:
//		Type id of left operand
//
//---------------------------------------------------------------------------
IMDId *
CMDScalarOpGPDB::PmdidTypeLeft() const
{
	return m_pmdidTypeLeft;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::PmdidTypeRight
//
//	@doc:
//		Type id of right operand
//
//---------------------------------------------------------------------------
IMDId *
CMDScalarOpGPDB::PmdidTypeRight() const
{
	return m_pmdidTypeRight;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::PmdidTypeResult
//
//	@doc:
//		Type id of result
//
//---------------------------------------------------------------------------
IMDId *
CMDScalarOpGPDB::PmdidTypeResult() const
{
	return m_pmdidTypeResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::PmdidFunc
//
//	@doc:
//		Id of function which implements the operator
//
//---------------------------------------------------------------------------
IMDId *
CMDScalarOpGPDB::PmdidFunc() const
{
	return m_pmdidFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::PmdidOpCommute
//
//	@doc:
//		Id of commute operator
//
//---------------------------------------------------------------------------
IMDId *
CMDScalarOpGPDB::PmdidOpCommute() const
{
	return m_pmdidOpCommute;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::PmdidOpInverse
//
//	@doc:
//		Id of inverse operator
//
//---------------------------------------------------------------------------
IMDId *
CMDScalarOpGPDB::PmdidOpInverse() const
{
	return m_pmdidOpInverse;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::FEquality
//
//	@doc:
//		Is this an equality operator
//
//---------------------------------------------------------------------------
BOOL
CMDScalarOpGPDB::FEquality() const
{
	return IMDType::EcmptEq == m_ecmpt;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::FReturnsNullOnNullInput
//
//	@doc:
//		Does operator return NULL when all inputs are NULL?
//		STRICT implies NULL-returning, but the opposite is not always true,
//		the implementation in GPDB returns what STRICT property states
//
//---------------------------------------------------------------------------
BOOL
CMDScalarOpGPDB::FReturnsNullOnNullInput() const
{
	return m_fReturnsNullOnNullInput;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::Ecmpt
//
//	@doc:
//		Comparison type
//
//---------------------------------------------------------------------------
IMDType::ECmpType
CMDScalarOpGPDB::Ecmpt() const
{
	return m_ecmpt;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::Serialize
//
//	@doc:
//		Serialize scalar op metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDScalarOpGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBScalarOp));
	
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBScalarOpCmpType), IMDType::PstrCmpType(m_ecmpt));
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenReturnsNullOnNullInput), m_fReturnsNullOnNullInput);

	Edxltoken rgEdxltoken[6] = {
							EdxltokenGPDBScalarOpLeftTypeId, EdxltokenGPDBScalarOpRightTypeId, 
							EdxltokenGPDBScalarOpResultTypeId, EdxltokenGPDBScalarOpFuncId, 
							EdxltokenGPDBScalarOpCommOpId, EdxltokenGPDBScalarOpInverseOpId
							};
	
	IMDId *rgMdid[6] = {m_pmdidTypeLeft, m_pmdidTypeRight, m_pmdidTypeResult, 
						m_pmdidFunc, m_pmdidOpCommute, m_pmdidOpInverse};
	
	for (ULONG ul = 0; ul < GPOS_ARRAY_SIZE(rgEdxltoken); ul++)
	{
		SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(rgEdxltoken[ul]), rgMdid[ul]);

		GPOS_CHECK_ABORT;
	}	
	
	// serialize operator class information
	if (0 < m_pdrgpmdidOpClasses->UlLength())
	{
		SerializeMDIdList(pxmlser, m_pdrgpmdidOpClasses, 
						CDXLTokens::PstrToken(EdxltokenOpClasses), 
						CDXLTokens::PstrToken(EdxltokenOpClass));
	}
	
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBScalarOp));
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::UlOpCLasses
//
//	@doc:
//		Number of classes this operator belongs to
//
//---------------------------------------------------------------------------
ULONG
CMDScalarOpGPDB::UlOpCLasses() const
{
	return m_pdrgpmdidOpClasses->UlLength();
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::PmdidOpClass
//
//	@doc:
//		Operator class at given position
//
//---------------------------------------------------------------------------
IMDId *
CMDScalarOpGPDB::PmdidOpClass
	(
	ULONG ulPos
	) 
	const
{
	GPOS_ASSERT(ulPos < m_pdrgpmdidOpClasses->UlLength());
	
	return (*m_pdrgpmdidOpClasses)[ulPos];
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDScalarOpGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDScalarOpGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Operator id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;
	
	os << "Operator name: " << (Mdname()).Pstr()->Wsz() << std::endl;
	
	os << "Left operand type id: ";
	PmdidTypeLeft()->OsPrint(os);
	os << std::endl;
		
	os << "Right operand type id: ";
	PmdidTypeRight()->OsPrint(os);
	os << std::endl;

	os << "Result type id: ";
	PmdidTypeResult()->OsPrint(os);
	os << std::endl;

	os << "Operator func id: ";
	PmdidFunc()->OsPrint(os);
	os << std::endl;

	os << "Commute operator id: ";
	PmdidOpCommute()->OsPrint(os);
	os << std::endl;

	os << "Inverse operator id: ";
	PmdidOpInverse()->OsPrint(os);
	os << std::endl;

	os << std::endl;	
}

#endif // GPOS_DEBUG


// EOF
