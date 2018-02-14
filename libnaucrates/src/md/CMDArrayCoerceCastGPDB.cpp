//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDArrayCoerceCastGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific array coerce
//		casts in the MD cache
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDArrayCoerceCastGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpmd;
using namespace gpdxl;

// ctor
CMDArrayCoerceCastGPDB::CMDArrayCoerceCastGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	IMDId *pmdidSrc,
	IMDId *pmdidDest,
	BOOL fBinaryCoercible,
	IMDId *pmdidCastFunc,
	EmdCoercepathType emdPathType,
	INT iTypeModifier,
	BOOL fIsExplicit,
	EdxlCoercionForm edxlcf,
	INT iLoc
	)
	:
	CMDCastGPDB(pmp, pmdid, pmdname, pmdidSrc, pmdidDest, fBinaryCoercible, pmdidCastFunc, emdPathType),
	m_iTypeModifier(iTypeModifier),
	m_fIsExplicit(fIsExplicit),
	m_edxlcf(edxlcf),
	m_iLoc(iLoc)
{
	m_pstr = CDXLUtils::PstrSerializeMDObj(pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

// dtor
CMDArrayCoerceCastGPDB::~CMDArrayCoerceCastGPDB()
{
	GPOS_DELETE(m_pstr);
}

// return type modifier
INT
CMDArrayCoerceCastGPDB::ITypeModifier() const
{
	return m_iTypeModifier;
}

// return is explicit cast
BOOL
CMDArrayCoerceCastGPDB::FIsExplicit() const
{
	return m_fIsExplicit;
}

// return coercion form
EdxlCoercionForm
CMDArrayCoerceCastGPDB::Ecf() const
{
	return m_edxlcf;
}

// return token location
INT
CMDArrayCoerceCastGPDB::ILoc() const
{
	return m_iLoc;
}

// serialize function metadata in DXL format
void
CMDArrayCoerceCastGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	)
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenGPDBArrayCoerceCast));

	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBCastCoercePathType), m_emdPathType);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBCastBinaryCoercible), m_fBinaryCoercible);

	m_pmdidSrc->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBCastSrcType));
	m_pmdidDest->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBCastDestType));
	m_pmdidCastFunc->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBCastFuncId));

	if (IDefaultTypeModifier != ITypeModifier())
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeMod), ITypeModifier());
	}

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenIsExplicit), m_fIsExplicit);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenCoercionForm), (ULONG) m_edxlcf);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenLocation), m_iLoc);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
						CDXLTokens::PstrToken(EdxltokenGPDBArrayCoerceCast));
}


#ifdef GPOS_DEBUG

// prints a metadata cache relation to the provided output
void
CMDArrayCoerceCastGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	CMDCastGPDB::DebugPrint(os);
	os << ", Result Type Mod: ";
	os << m_iTypeModifier;
	os << ", isExplicit: ";
	os << m_fIsExplicit;
	os << ", coercion form: ";
	os << m_edxlcf;

	os << std::endl;
}

#endif // GPOS_DEBUG

// EOF
