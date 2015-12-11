//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDScCmpGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific comparisons
//		in the MD cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDScCmpGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpmd;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::CMDScCmpGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDScCmpGPDB::CMDScCmpGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	IMDId *pmdidLeft,
	IMDId *pmdidRight,
	IMDType::ECmpType ecmpt,
	IMDId *pmdidOp
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_pmdidLeft(pmdidLeft),
	m_pmdidRight(pmdidRight),
	m_ecmpt(ecmpt),
	m_pmdidOp(pmdidOp)
{
	GPOS_ASSERT(m_pmdid->FValid());
	GPOS_ASSERT(m_pmdidLeft->FValid());
	GPOS_ASSERT(m_pmdidRight->FValid());
	GPOS_ASSERT(m_pmdidOp->FValid());
	GPOS_ASSERT(IMDType::EcmptOther != m_ecmpt);

	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::~CMDScCmpGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDScCmpGPDB::~CMDScCmpGPDB()
{
	m_pmdid->Release();
	m_pmdidLeft->Release();
	m_pmdidRight->Release();
	m_pmdidOp->Release();
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::Pmdid
//
//	@doc:
//		Mdid of comparison object
//
//---------------------------------------------------------------------------
IMDId *
CMDScCmpGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::Mdname
//
//	@doc:
//		Operator name
//
//---------------------------------------------------------------------------
CMDName
CMDScCmpGPDB::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::PmdidLeft
//
//	@doc:
//		Left type id
//
//---------------------------------------------------------------------------
IMDId *
CMDScCmpGPDB::PmdidLeft() const
{
	return m_pmdidLeft;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::PmdidRight
//
//	@doc:
//		Destination type id
//
//---------------------------------------------------------------------------
IMDId *
CMDScCmpGPDB::PmdidRight() const
{
	return m_pmdidRight;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::PmdidOp
//
//	@doc:
//		Cast function id
//
//---------------------------------------------------------------------------
IMDId *
CMDScCmpGPDB::PmdidOp() const
{
	return m_pmdidOp;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::FBinaryCoercible
//
//	@doc:
//		Returns whether this is a cast between binary coercible types, i.e. the 
//		types are binary compatible
//
//---------------------------------------------------------------------------
IMDType::ECmpType
CMDScCmpGPDB::Ecmpt() const
{
	return m_ecmpt;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::Serialize
//
//	@doc:
//		Serialize comparison operator metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDScCmpGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBMDScCmp));
	
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBScalarOpCmpType), IMDType::PstrCmpType(m_ecmpt));

	m_pmdidLeft->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBScalarOpLeftTypeId));
	m_pmdidRight->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBScalarOpRightTypeId));
	m_pmdidOp->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenOpNo));

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBMDScCmp));

	GPOS_CHECK_ABORT;
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDScCmpGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDScCmpGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "ComparisonOp ";
	PmdidLeft()->OsPrint(os);
	os << (Mdname()).Pstr()->Wsz() << "(";
	PmdidOp()->OsPrint(os);
	os << ") ";
	PmdidLeft()->OsPrint(os);


	os << ", type: " << IMDType::PstrCmpType(m_ecmpt);

	os << std::endl;	
}

#endif // GPOS_DEBUG

// EOF
