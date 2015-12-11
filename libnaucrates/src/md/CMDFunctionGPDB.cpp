//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDFunctionGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific functions
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

#include "naucrates/md/CMDFunctionGPDB.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpmd;
using namespace gpdxl;

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::CMDFunctionGPDB
//
//	@doc:
//		Constructs a metadata func
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::CMDFunctionGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	IMDId *pmdidTypeResult,
	DrgPmdid *pdrgpmdidTypes,
	BOOL fReturnsSet,
	EFuncStbl efsStability,
	EFuncDataAcc efdaDataAccess,
	BOOL fStrict
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_pmdidTypeResult(pmdidTypeResult),
	m_pdrgpmdidTypes(pdrgpmdidTypes),
	m_fReturnsSet(fReturnsSet),
	m_efsStability(efsStability),
	m_efdaDataAccess(efdaDataAccess),
	m_fStrict(fStrict)
{
	GPOS_ASSERT(m_pmdid->FValid());
	GPOS_ASSERT(EfsSentinel > efsStability);
	GPOS_ASSERT(EfdaSentinel > efdaDataAccess);

	InitDXLTokenArrays();
	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::~CMDFunctionGPDB
//
//	@doc:
//		Destructor
//
//---------------------------------------------------------------------------
CMDFunctionGPDB::~CMDFunctionGPDB()
{
	m_pmdid->Release();
	m_pmdidTypeResult->Release();
	CRefCount::SafeRelease(m_pdrgpmdidTypes);
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::InitDXLTokenArrays
//
//	@doc:
//		Initialize DXL token arrays
//
//---------------------------------------------------------------------------
void
CMDFunctionGPDB::InitDXLTokenArrays()
{
	// stability
	m_drgDxlStability[EfsImmutable] = EdxltokenGPDBFuncImmutable;
	m_drgDxlStability[EfsStable] = EdxltokenGPDBFuncStable;
	m_drgDxlStability[EfsVolatile] = EdxltokenGPDBFuncVolatile;

	// data access
	m_drgDxlDataAccess[EfdaNoSQL] = EdxltokenGPDBFuncNoSQL;
	m_drgDxlDataAccess[EfdaContainsSQL] = EdxltokenGPDBFuncContainsSQL;
	m_drgDxlDataAccess[EfdaReadsSQLData] = EdxltokenGPDBFuncReadsSQLData;
	m_drgDxlDataAccess[EfdaModifiesSQLData] = EdxltokenGPDBFuncModifiesSQLData;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::Pmdid
//
//	@doc:
//		Func id
//
//---------------------------------------------------------------------------
IMDId *
CMDFunctionGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::Mdname
//
//	@doc:
//		Func name
//
//---------------------------------------------------------------------------
CMDName
CMDFunctionGPDB::Mdname() const
{
	return *m_pmdname;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::PmdidTypeResult
//
//	@doc:
//		Type id of result
//
//---------------------------------------------------------------------------
IMDId *
CMDFunctionGPDB::PmdidTypeResult() const
{
	return m_pmdidTypeResult;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::PdrgpmdidOutputArgTypes
//
//	@doc:
//		Output argument types
//
//---------------------------------------------------------------------------
DrgPmdid *
CMDFunctionGPDB::PdrgpmdidOutputArgTypes() const
{
	return m_pdrgpmdidTypes;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::FReturnsSet
//
//	@doc:
//		Returns whether function result is a set
//
//---------------------------------------------------------------------------
BOOL
CMDFunctionGPDB::FReturnsSet() const
{
	return m_fReturnsSet;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::PstrOutArgTypes
//
//	@doc:
//		Serialize the array of output argument types into a comma-separated string
//
//---------------------------------------------------------------------------
CWStringDynamic *
CMDFunctionGPDB::PstrOutArgTypes() const
{
	GPOS_ASSERT(NULL != m_pdrgpmdidTypes);
	CWStringDynamic *pstr = GPOS_NEW(m_pmp) CWStringDynamic(m_pmp);

	const ULONG ulLen = m_pdrgpmdidTypes->UlLength();
	for (ULONG ul = 0; ul < ulLen; ul++)
	{
		IMDId *pmdid = (*m_pdrgpmdidTypes)[ul];
		if (ul == ulLen - 1)
		{
			// last element: do not print a comma
			pstr->AppendFormat(GPOS_WSZ_LIT("%ls"), pmdid->Wsz());
		}
		else
		{
			pstr->AppendFormat(GPOS_WSZ_LIT("%ls%ls"), pmdid->Wsz(), CDXLTokens::PstrToken(EdxltokenComma)->Wsz());
		}
	}

	return pstr;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::Serialize
//
//	@doc:
//		Serialize function metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDFunctionGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBFunc));
	
	m_pmdid->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));

	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pmdname->Pstr());
	
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBFuncReturnsSet), m_fReturnsSet);
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBFuncStability), PstrStability());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBFuncDataAccess), PstrDataAccess());
	pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenGPDBFuncStrict), m_fStrict);

	SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenGPDBFuncResultTypeId), m_pmdidTypeResult);

	if (NULL != m_pdrgpmdidTypes)
	{
		pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
							CDXLTokens::PstrToken(EdxltokenOutputCols));

		CWStringDynamic *pstrOutArgTypes = PstrOutArgTypes();
		pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenTypeIds), pstrOutArgTypes);
		GPOS_DELETE(pstrOutArgTypes);

		pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
							CDXLTokens::PstrToken(EdxltokenOutputCols));

	}
	pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), 
						CDXLTokens::PstrToken(EdxltokenGPDBFunc));
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::PstrStability
//
//	@doc:
//		String representation of function stability
//
//---------------------------------------------------------------------------
const CWStringConst *
CMDFunctionGPDB::PstrStability() const
{
	if (EfsSentinel > m_efsStability)
	{
		return CDXLTokens::PstrToken(m_drgDxlStability[m_efsStability]);
	}

	GPOS_ASSERT(!"Unrecognized function stability setting");
	return NULL;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::PstrDataAccess
//
//	@doc:
//		String representation of function data access
//
//---------------------------------------------------------------------------
const CWStringConst *
CMDFunctionGPDB::PstrDataAccess() const
{
	if (EfdaSentinel > m_efdaDataAccess)
	{
		return CDXLTokens::PstrToken(m_drgDxlDataAccess[m_efdaDataAccess]);
	}

	GPOS_ASSERT(!"Unrecognized function data access setting");
	return NULL;
}

#ifdef GPOS_DEBUG

//---------------------------------------------------------------------------
//	@function:
//		CMDFunctionGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDFunctionGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	os << "Function id: ";
	Pmdid()->OsPrint(os);
	os << std::endl;
	
	os << "Function name: " << (Mdname()).Pstr()->Wsz() << std::endl;
	
	os << "Result type id: ";
	PmdidTypeResult()->OsPrint(os);
	os << std::endl;
	
	const CWStringConst *pstrReturnsSet = FReturnsSet() ? 
			CDXLTokens::PstrToken(EdxltokenTrue): 
			CDXLTokens::PstrToken(EdxltokenFalse); 

	os << "Returns set: " << pstrReturnsSet->Wsz() << std::endl;

	os << "Function is " << PstrStability()->Wsz() << std::endl;
	
	os << "Data access: " << PstrDataAccess()->Wsz() << std::endl;

	const CWStringConst *pstrIsStrict = FStrict() ? 
			CDXLTokens::PstrToken(EdxltokenTrue): 
			CDXLTokens::PstrToken(EdxltokenFalse); 

	os << "Is strict: " << pstrIsStrict->Wsz() << std::endl;
	
	os << std::endl;	
}

#endif // GPOS_DEBUG

// EOF
