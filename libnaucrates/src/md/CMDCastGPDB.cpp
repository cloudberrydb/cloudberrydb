//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CMDCastGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific casts
//		in the MD cache
//---------------------------------------------------------------------------


#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDCastGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpmd;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::CMDCastGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDCastGPDB::CMDCastGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	IMDId *pmdidSrc,
	IMDId *pmdidDest,
	BOOL fBinaryCoercible,
	IMDId *pmdidCastFunc
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_pmdidSrc(pmdidSrc),
	m_pmdidDest(pmdidDest),
	m_fBinaryCoercible(fBinaryCoercible),
	m_pmdidCastFunc(pmdidCastFunc)
{
	GPOS_ASSERT(m_pmdid->FValid());
	GPOS_ASSERT(m_pmdidSrc->FValid());
	GPOS_ASSERT(m_pmdidDest->FValid());
	GPOS_ASSERT_IMP(!fBinaryCoercible, m_pmdidCastFunc->FValid());

	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::~CMDCastGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDCastGPDB::~CMDCastGPDB()
{
	m_pmdid->Release();
	m_pmdidSrc->Release();
	m_pmdidDest->Release();
	CRefCount::SafeRelease(m_pmdidCastFunc);
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::Pmdid
//
//	@doc:
//		Mdid of cast object
//
//---------------------------------------------------------------------------
IMDId *
CMDCastGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::Mdname
//
//	@doc:
//		Func name
//
//---------------------------------------------------------------------------
CMDName
CMDCastGPDB::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::PmdidSrc
//
//	@doc:
//		Source type id
//
//---------------------------------------------------------------------------
IMDId *
CMDCastGPDB::PmdidSrc() const
{
	return m_pmdidSrc;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::PmdidDest
//
//	@doc:
//		Destination type id
//
//---------------------------------------------------------------------------
IMDId *
CMDCastGPDB::PmdidDest() const
{
	return m_pmdidDest;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::PmdidCastFunc
//
//	@doc:
//		Cast function id
//
//---------------------------------------------------------------------------
IMDId *
CMDCastGPDB::PmdidCastFunc() const
{
	return m_pmdidCastFunc;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::FBinaryCoercible
//
//	@doc:
//		Returns whether this is a cast between binary coercible types, i.e. the 
//		types are binary compatible
//
//---------------------------------------------------------------------------
BOOL
CMDCastGPDB::FBinaryCoercible() const
{
	return m_fBinaryCoercible;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::Serialize
//
//	@doc:
//		Serialize function metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDCastGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBCast));
	
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBCastBinaryCoercible), m_fBinaryCoercible);

	m_pmdidSrc->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBCastSrcType));
	m_pmdidDest->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBCastDestType));
	m_pmdidCastFunc->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBCastFuncId));

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBCast));
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDCastGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDCastGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Cast " << (Mdname()).Pstr()->Wsz() << ": ";
	PmdidSrc()->OsPrint(os);
	os << "->";
	PmdidDest()->OsPrint(os);
	os << std::endl;
		
	if (m_fBinaryCoercible)
	{
		os << ", binary-coercible";
	}
	
	if (IMDId::FValid(m_pmdidCastFunc))
	{
		os << ", Cast func id: ";
		PmdidCastFunc()->OsPrint(os);
	}
	
	os << std::endl;	
}

#endif // GPOS_DEBUG

// EOF
