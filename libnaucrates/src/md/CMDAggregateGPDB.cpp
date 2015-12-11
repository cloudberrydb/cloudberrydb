//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDAggregateGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific aggregates
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

#include "naucrates/md/CMDAggregateGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpmd;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateGPDB::CMDAggregateGPDB
//
//	@doc:
//		Constructs a metadata aggregate
//
//---------------------------------------------------------------------------
CMDAggregateGPDB::CMDAggregateGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	IMDId *pmdidTypeResult,
	IMDId *pmdidTypeIntermediate,
	BOOL fOrdered,
	BOOL fSplittable,
	BOOL fHashAggCapable
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_pmdidTypeResult(pmdidTypeResult),
	m_pmdidTypeIntermediate(pmdidTypeIntermediate),
	m_fOrdered(fOrdered),
	m_fSplittable(fSplittable),
	m_fHashAggCapable(fHashAggCapable)
	{
		GPOS_ASSERT(pmdid->FValid());
		
		m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
	}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateGPDB::~CMDAggregateGPDB
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CMDAggregateGPDB::~CMDAggregateGPDB()
{
	m_pmdid->Release();
	m_pmdidTypeIntermediate->Release();
	m_pmdidTypeResult->Release();
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateGPDB::Pmdid
//
//	@doc:
//		Agg id
//
//---------------------------------------------------------------------------
IMDId *
CMDAggregateGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateGPDB::Mdname
//
//	@doc:
//		Agg name
//
//---------------------------------------------------------------------------
CMDName
CMDAggregateGPDB::Mdname() const
{
	return *m_pmdname;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateGPDB::PmdidTypeResult
//
//	@doc:
//		Type id of result
//
//---------------------------------------------------------------------------
IMDId *
CMDAggregateGPDB::PmdidTypeResult() const
{
	return m_pmdidTypeResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateGPDB::PmdidTypeIntermediate
//
//	@doc:
//		Type id of intermediate result
//
//---------------------------------------------------------------------------
IMDId *
CMDAggregateGPDB::PmdidTypeIntermediate() const
{
	return m_pmdidTypeIntermediate;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateGPDB::Serialize
//
//	@doc:
//		Serialize function metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDAggregateGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBAgg));
	
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	if (m_fOrdered)
	{
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBIsAggOrdered), m_fOrdered);
	}
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBAggSplittable), m_fSplittable);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBAggHashAggCapable), m_fHashAggCapable);
	
	SerializeMDIdAsElem(pxmlser, 
			CDXLTokens::PstrToken(EdxltokenGPDBAggResultTypeId), m_pmdidTypeResult);
	SerializeMDIdAsElem(pxmlser, 
			CDXLTokens::PstrToken(EdxltokenGPDBAggIntermediateResultTypeId), m_pmdidTypeIntermediate);

	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBAgg));
}


#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDAggregateGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDAggregateGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Aggregate id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;
	
	os << "Aggregate name: " << (Mdname()).Pstr()->Wsz() << std::endl;
	
	os << "Result type id: ";
	PmdidTypeResult()->OsPrint(os);
	os << std::endl;
	
	os << "Intermediate result type id: ";
	PmdidTypeIntermediate()->OsPrint(os);
	os << std::endl;
	
	os << std::endl;	
}

#endif // GPOS_DEBUG

// EOF
