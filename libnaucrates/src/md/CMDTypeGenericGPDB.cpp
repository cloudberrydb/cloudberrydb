//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDTypeGenericGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB generic types
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDTypeGenericGPDB.h"
#include "naucrates/md/CGPDBTypeHelper.h"

#include "naucrates/base/CDatumGenericGPDB.h"

#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/operators/CDXLDatumStatsDoubleMappable.h"
#include "naucrates/dxl/operators/CDXLDatumStatsLintMappable.h"
#include "naucrates/dxl/operators/CDXLDatumGeneric.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"

using namespace gpnaucrates;
using namespace gpdxl;
using namespace gpmd;

#define GPDB_ANYELEMENT_OID OID(2283) // oid of GPDB ANYELEMENT type

#define GPDB_ANYARRAY_OID OID(2277) // oid of GPDB ANYARRAY type

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::CMDTypeGenericGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDTypeGenericGPDB::CMDTypeGenericGPDB
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	CMDName *pmdname,
	BOOL fRedistributable,
	BOOL fFixedLength,
	ULONG ulLength, 
	BOOL fByValue,
	IMDId *pmdidOpEq,
	IMDId *pmdidOpNeq,
	IMDId *pmdidOpLT,
	IMDId *pmdidOpLEq,
	IMDId *pmdidOpGT,
	IMDId *pmdidOpGEq,
	IMDId *pmdidOpComp,
	IMDId *pmdidMin,
	IMDId *pmdidMax,
	IMDId *pmdidAvg,
	IMDId *pmdidSum,
	IMDId *pmdidCount,
	BOOL fHashable,
	BOOL fComposite,
	IMDId *pmdidBaseRelation,
	IMDId *pmdidTypeArray,
	INT iLength
	)
	:
	m_pmp(pmp),
	m_pmdid(pmdid),
	m_pmdname(pmdname),
	m_fRedistributable(fRedistributable),
	m_fFixedLength(fFixedLength),
	m_ulLength(ulLength),
	m_fByValue(fByValue),
	m_pmdidOpEq(pmdidOpEq),
	m_pmdidOpNeq(pmdidOpNeq),
	m_pmdidOpLT(pmdidOpLT),
	m_pmdidOpLEq(pmdidOpLEq),
	m_pmdidOpGT(pmdidOpGT),
	m_pmdidOpGEq(pmdidOpGEq),
	m_pmdidOpComp(pmdidOpComp),
	m_pmdidMin(pmdidMin),
	m_pmdidMax(pmdidMax),
	m_pmdidAvg(pmdidAvg),
	m_pmdidSum(pmdidSum),
	m_pmdidCount(pmdidCount),
	m_fHashable(fHashable),
	m_fComposite(fComposite),
	m_pmdidBaseRelation(pmdidBaseRelation),
	m_pmdidTypeArray(pmdidTypeArray),
	m_iLength(iLength),
	m_pdatumNull(NULL)
{
	GPOS_ASSERT_IMP(m_fFixedLength, 0 < m_ulLength);
	GPOS_ASSERT_IMP(!m_fFixedLength, 0 > m_iLength);
	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);

	m_pmdid->AddRef();
	m_pdatumNull = GPOS_NEW(m_pmp) CDatumGenericGPDB(m_pmp, m_pmdid, NULL /*pba*/, 0 /*ulLength*/, true /*fConstNull*/, 0 /*lValue */, 0 /*dValue */);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::~CMDTypeGenericGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDTypeGenericGPDB::~CMDTypeGenericGPDB()
{
	m_pmdid->Release();
	m_pmdidOpEq->Release();
	m_pmdidOpNeq->Release();
	m_pmdidOpLT->Release();
	m_pmdidOpLEq->Release();
	m_pmdidOpGT->Release();
	m_pmdidOpGEq->Release();
	m_pmdidOpComp->Release();
	m_pmdidTypeArray->Release();
	m_pmdidMin->Release();
	m_pmdidMax->Release();
	m_pmdidAvg->Release();
	m_pmdidSum->Release();
	m_pmdidCount->Release();
	CRefCount::SafeRelease(m_pmdidBaseRelation);
	GPOS_DELETE(m_pmdname);
	GPOS_DELETE(m_pstr);
	m_pdatumNull->Release();
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::PmdidAgg
//
//	@doc:
//		Return mdid of specified aggregate type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeGenericGPDB::PmdidAgg
	(
	EAggType eagg
	) 
	const
{
	switch (eagg)
	{
		case EaggMin:
			return m_pmdidMin;
		case EaggMax:
			return m_pmdidMax;
		case EaggAvg:
			return m_pmdidAvg;
		case EaggSum:
			return m_pmdidSum;
		case EaggCount:
			return m_pmdidCount;
		default:
			GPOS_ASSERT(!"Invalid aggregate type");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeGenericGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::Mdname
//
//	@doc:
//		Returns the name of this type
//
//---------------------------------------------------------------------------
CMDName
CMDTypeGenericGPDB::Mdname() const
{
	return *m_pmdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::PmdidCmp
//
//	@doc:
//		Return mdid of specified comparison operator type
//
//---------------------------------------------------------------------------
IMDId * 
CMDTypeGenericGPDB::PmdidCmp
	(
	ECmpType ecmpt
	) 
	const
{
	switch (ecmpt)
	{
	case EcmptEq:
		return m_pmdidOpEq;
	case EcmptNEq:
		return m_pmdidOpNeq;
	case EcmptL:
		return m_pmdidOpLT;
	case EcmptLEq: 
		return m_pmdidOpLEq;
	case EcmptG:
		return m_pmdidOpGT;
	case EcmptGEq:
		return m_pmdidOpGEq;
	default:
		GPOS_ASSERT(!"Invalid operator type");
		return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::Serialize
//
//	@doc:
//		Serialize metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDTypeGenericGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	CGPDBTypeHelper<CMDTypeGenericGPDB>::Serialize(pxmlser, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::Pdatum
//
//	@doc:
//		Factory method for generating generic datum from CDXLScalarConstValue
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeGenericGPDB::Pdatum
	(
	const CDXLScalarConstValue *pdxlop
	)
	const
{
	CDXLDatumGeneric *pdxldatum = CDXLDatumGeneric::PdxldatumConvert(const_cast<CDXLDatum*>(pdxlop->Pdxldatum()));
	GPOS_ASSERT(NULL != pdxlop);

	LINT lValue = 0;
	if (pdxldatum->FHasStatsLINTMapping())
	{
		lValue = pdxldatum->LStatsMapping();
	}

	CDouble dValue = 0;
	if (pdxldatum->FHasStatsDoubleMapping())
	{
		dValue = pdxldatum->DStatsMapping();
	}

	m_pmdid->AddRef();
	return GPOS_NEW(m_pmp) CDatumGenericGPDB(m_pmp, m_pmdid, pdxldatum->Pba(), pdxldatum->UlLength(), pdxldatum->FNull(), lValue, dValue);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::Pdatum
//
//	@doc:
//		Construct a datum from a DXL datum
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeGenericGPDB::Pdatum
	(
	IMemoryPool *pmp,
	const CDXLDatum *pdxldatum
	)
	const
{
	m_pmdid->AddRef();
	CDXLDatumGeneric *pdxldatumGeneric = CDXLDatumGeneric::PdxldatumConvert(const_cast<CDXLDatum *>(pdxldatum));

	LINT lValue = 0;
	if (pdxldatumGeneric->FHasStatsLINTMapping())
	{
		lValue = pdxldatumGeneric->LStatsMapping();
	}

	CDouble dValue = 0;
	if (pdxldatumGeneric->FHasStatsDoubleMapping())
	{
		dValue = pdxldatumGeneric->DStatsMapping();
	}

	return GPOS_NEW(m_pmp) CDatumGenericGPDB
						(
						pmp,
						m_pmdid,
						pdxldatumGeneric->Pba(),
						pdxldatumGeneric->UlLength(),
						pdxldatumGeneric->FNull(),
						lValue,
						dValue
						);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::Pdxldatum
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeGenericGPDB::Pdxldatum
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	m_pmdid->AddRef();
	CDatumGenericGPDB *pdatumgeneric = dynamic_cast<CDatumGenericGPDB*>(pdatum);
	ULONG ulLength = 0;
	BYTE *pba = NULL;
	if (!pdatumgeneric->FNull())
	{
		pba = pdatumgeneric->PbaVal(pmp, &ulLength);
	}

	LINT lValue = 0;
	if (pdatumgeneric->FHasStatsLINTMapping())
	{
		lValue = pdatumgeneric->LStatsMapping();
	}

	CDouble dValue = 0;
	if (pdatumgeneric->FHasStatsDoubleMapping())
	{
		dValue = pdatumgeneric->DStatsMapping();
	}

	return Pdxldatum(pmp, m_pmdid, m_fByValue, pdatumgeneric->FNull(), pba, ulLength, lValue, dValue);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::FAmbiguous
//
//	@doc:
// 		Is type an ambiguous one? e.g., AnyElement/AnyArray in GPDB
//
//---------------------------------------------------------------------------
BOOL
CMDTypeGenericGPDB::FAmbiguous() const
{
	OID oid = CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId();
	return (GPDB_ANYELEMENT_OID == oid || GPDB_ANYARRAY_OID == oid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::Pdxldatum
//
//	@doc:
// 		Create a dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeGenericGPDB::Pdxldatum
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	BOOL fByVal,
	BOOL fNull,
	BYTE *pba,
	ULONG ulLength,
	LINT lValue,
	CDouble dValue
	)
{
	SGenericDXLDatumMap rgGenericDXLDatumMap[] =
		{
			// numbers
			{&CMDIdGPDB::m_mdidNumeric , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidFloat4 , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidFloat8 , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			// has lint mapping
			{&CMDIdGPDB::m_mdidBPChar  , &CMDTypeGenericGPDB::PdxldatumStatsLintMappable},
			{&CMDIdGPDB::m_mdidVarChar  , &CMDTypeGenericGPDB::PdxldatumStatsLintMappable},
			{&CMDIdGPDB::m_mdidText  , &CMDTypeGenericGPDB::PdxldatumStatsLintMappable},
			{&CMDIdGPDB::m_mdidCash  , &CMDTypeGenericGPDB::PdxldatumStatsLintMappable},
			// time-related types
			{&CMDIdGPDB::m_mdidDate , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidTime , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidTimeTz , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidTimestamp , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidTimestampTz , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidAbsTime , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidRelTime , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidInterval , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidTimeInterval , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			// network-related types
			{&CMDIdGPDB::m_mdidInet , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidCidr , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable},
			{&CMDIdGPDB::m_mdidMacaddr , &CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable}
		};

	const ULONG ulMaps = GPOS_ARRAY_SIZE(rgGenericDXLDatumMap);
	PfPdxldatum *pf = NULL;
	for (ULONG ul = 0; ul < ulMaps; ul++)
	{
		SGenericDXLDatumMap elem = rgGenericDXLDatumMap[ul];
		if (pmdid->FEquals(elem.m_pmdid))
		{
			pf = elem.m_pf;
			break;
		}
	}

	if (NULL != pf)
	{
		return (*pf)(pmp, pmdid, fByVal, fNull, pba, ulLength, lValue, dValue);
	}

	return GPOS_NEW(pmp) CDXLDatumGeneric(pmp, pmdid, fByVal, fNull, pba, ulLength);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable
//
//	@doc:
// 		Create a dxl datum of types that need double mapping
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeGenericGPDB::PdxldatumStatsDoubleMappable
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	BOOL fByValue,
	BOOL fNull,
	BYTE *pba,
	ULONG ulLength,
	LINT ,
	CDouble dValue
	)
{
	GPOS_ASSERT(CMDTypeGenericGPDB::FHasByteDoubleMapping(pmdid));
	return GPOS_NEW(pmp) CDXLDatumStatsDoubleMappable(pmp, pmdid, fByValue, fNull, pba, ulLength, dValue);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::PdxldatumStatsLintMappable
//
//	@doc:
// 		Create a dxl datum of types having lint mapping
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeGenericGPDB::PdxldatumStatsLintMappable
	(
	IMemoryPool *pmp,
	IMDId *pmdid,
	BOOL fByValue,
	BOOL fNull,
	BYTE *pba,
	ULONG ulLength,
	LINT lValue,
	CDouble // dValue
	)
{
	GPOS_ASSERT(CMDTypeGenericGPDB::FHasByteLintMapping(pmdid));
	return GPOS_NEW(pmp) CDXLDatumStatsLintMappable(pmp, pmdid, fByValue, fNull, pba, ulLength, lValue);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::PdxlopScConst
//
//	@doc:
// 		Generate a dxl scalar constant from a datum
//
//---------------------------------------------------------------------------
CDXLScalarConstValue *
CMDTypeGenericGPDB::PdxlopScConst
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	CDXLDatum *pdxldatum = Pdxldatum(pmp, pdatum);

	return GPOS_NEW(pmp) CDXLScalarConstValue(pmp, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::PdxldatumNull
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeGenericGPDB::PdxldatumNull
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();

	return Pdxldatum(pmp, m_pmdid, m_fByValue, true /*fConstNull*/, NULL /*pba*/, 0 /*ulLength*/, 0 /*lValue */, 0 /*dValue */);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::FHasByteLintMapping
//
//	@doc:
//		Does the datum of this type need bytea -> Lint mapping for
//		statistics computation
//---------------------------------------------------------------------------
BOOL
CMDTypeGenericGPDB::FHasByteLintMapping
	(
	const IMDId *pmdid
	)
{
	return pmdid->FEquals(&CMDIdGPDB::m_mdidBPChar)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidVarChar)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidText)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidCash);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::FHasByteDoubleMapping
//
//	@doc:
//		Does the datum of this type need bytea -> double mapping for
//		statistics computation
//---------------------------------------------------------------------------
BOOL
CMDTypeGenericGPDB::FHasByteDoubleMapping
	(
	const IMDId *pmdid
	)
{
	return pmdid->FEquals(&CMDIdGPDB::m_mdidNumeric)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidFloat4)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidFloat8)
			|| FTimeRelatedType(pmdid)
			|| FNetworkRelatedType(pmdid);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::FTimeRelatedType
//
//	@doc:
//		is this a time-related type
//---------------------------------------------------------------------------
BOOL
CMDTypeGenericGPDB::FTimeRelatedType
	(
	const IMDId *pmdid
	)
{
	return pmdid->FEquals(&CMDIdGPDB::m_mdidDate)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidTime)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidTimeTz)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidTimestamp)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidTimestampTz)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidAbsTime)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidRelTime)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidInterval)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidTimeInterval);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::FNetworkRelatedType
//
//	@doc:
//		is this a network-related type
//---------------------------------------------------------------------------
BOOL
CMDTypeGenericGPDB::FNetworkRelatedType
	(
	const IMDId *pmdid
	)
{
	return pmdid->FEquals(&CMDIdGPDB::m_mdidInet)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidCidr)
			|| pmdid->FEquals(&CMDIdGPDB::m_mdidMacaddr);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeGenericGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDTypeGenericGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	CGPDBTypeHelper<CMDTypeGenericGPDB>::DebugPrint(os, this);
}

#endif // GPOS_DEBUG

// EOF

