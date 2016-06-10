//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDTypeOidGPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific OID type in
//		the MD cache
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDTypeOidGPDB.h"
#include "naucrates/md/CGPDBTypeHelper.h"

#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/dxl/operators/CDXLDatumOid.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "naucrates/base/CDatumOidGPDB.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpnaucrates;

// static member initialization
CWStringConst
CMDTypeOidGPDB::m_str = CWStringConst(GPOS_WSZ_LIT("oid"));
CMDName
CMDTypeOidGPDB::m_mdname(&m_str);

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::CMDTypeOidGPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDTypeOidGPDB::CMDTypeOidGPDB
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{
	m_pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_OID);
	m_pmdidOpEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_EQ_OP);
	m_pmdidOpNeq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_NEQ_OP);
	m_pmdidOpLT = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_LT_OP);
	m_pmdidOpLEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_LEQ_OP);
	m_pmdidOpGT = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_GT_OP);
	m_pmdidOpGEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_GEQ_OP);
	m_pmdidOpComp = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_COMP_OP);
	m_pmdidTypeArray = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_ARRAY_TYPE);

	m_pmdidMin = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_AGG_MIN);
	m_pmdidMax = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_AGG_MAX);
	m_pmdidAvg = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_AGG_AVG);
	m_pmdidSum = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_AGG_SUM);
	m_pmdidCount = GPOS_NEW(pmp) CMDIdGPDB(GPDB_OID_AGG_COUNT);
	
	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);

	GPOS_ASSERT(GPDB_OID_OID == CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId());
	m_pmdid->AddRef();
	m_pdatumNull = GPOS_NEW(pmp) CDatumOidGPDB(m_pmdid, 1 /* fVal */, true /* fNull */);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::~CMDTypeOidGPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDTypeOidGPDB::~CMDTypeOidGPDB()
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
	m_pdatumNull->Release();

	GPOS_DELETE(m_pstr);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::Pdatum
//
//	@doc:
//		Factory function for creating OID datums
//
//---------------------------------------------------------------------------
IDatumOid *
CMDTypeOidGPDB::PdatumOid
	(
	IMemoryPool *pmp,
	OID oValue,
	BOOL fNULL
	)
	const
{
	return GPOS_NEW(pmp) CDatumOidGPDB(m_pmdid->Sysid(), oValue, fNULL);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeOidGPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::Mdname
//
//	@doc:
//		Returns the name of this type
//
//---------------------------------------------------------------------------
CMDName
CMDTypeOidGPDB::Mdname() const
{
	return CMDTypeOidGPDB::m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::PmdidCmp
//
//	@doc:
//		Return mdid of specified comparison operator type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeOidGPDB::PmdidCmp
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
//		CMDTypeOidGPDB::PmdidAgg
//
//	@doc:
//		Return mdid of specified aggregate type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeOidGPDB::PmdidAgg
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
//		CMDTypeOidGPDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDTypeOidGPDB::Serialize
	(
	CXMLSerializer *pxmlser
	)
	const
{
	CGPDBTypeHelper<CMDTypeOidGPDB>::Serialize(pxmlser, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::Pdatum
//
//	@doc:
//		Transformation method for generating oid datum from CDXLScalarConstValue
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeOidGPDB::Pdatum
	(
	const CDXLScalarConstValue *pdxlop
	)
	const
{
	CDXLDatumOid *pdxldatum = CDXLDatumOid::PdxldatumConvert(const_cast<CDXLDatum*>(pdxlop->Pdxldatum()));
	GPOS_ASSERT(pdxldatum->FByValue());

	return GPOS_NEW(m_pmp) CDatumOidGPDB(m_pmdid->Sysid(), pdxldatum->OidValue(), pdxldatum->FNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::Pdatum
//
//	@doc:
//		Construct an oid datum from a DXL datum
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeOidGPDB::Pdatum
	(
	IMemoryPool *pmp,
	const CDXLDatum *pdxldatum
	)
	const
{
	CDXLDatumOid *pdxldatumOid = CDXLDatumOid::PdxldatumConvert(const_cast<CDXLDatum *>(pdxldatum));
	GPOS_ASSERT(pdxldatumOid->FByValue());
	OID oValue = pdxldatumOid->OidValue();
	BOOL fNull = pdxldatumOid->FNull();

	return GPOS_NEW(pmp) CDatumOidGPDB(m_pmdid->Sysid(), oValue, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::Pdxldatum
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeOidGPDB::Pdxldatum
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	m_pmdid->AddRef();
	CDatumOidGPDB *pdatumOid = dynamic_cast<CDatumOidGPDB*>(pdatum);

	return GPOS_NEW(pmp) CDXLDatumOid(pmp, m_pmdid, pdatumOid->FNull(), pdatumOid->OidValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::PdxlopScConst
//
//	@doc:
// 		Generate a dxl scalar constant from a datum
//
//---------------------------------------------------------------------------
CDXLScalarConstValue *
CMDTypeOidGPDB::PdxlopScConst
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	CDatumOidGPDB *pdatumOidGPDB = dynamic_cast<CDatumOidGPDB *>(pdatum);

	m_pmdid->AddRef();
	CDXLDatumOid *pdxldatum = GPOS_NEW(pmp) CDXLDatumOid(pmp, m_pmdid, pdatumOidGPDB->FNull(), pdatumOidGPDB->OidValue());

	return GPOS_NEW(pmp) CDXLScalarConstValue(pmp, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::PdxldatumNull
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeOidGPDB::PdxldatumNull
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();

	return GPOS_NEW(pmp) CDXLDatumOid(pmp, m_pmdid, true /*fNull*/, 1);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeOidGPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDTypeOidGPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	CGPDBTypeHelper<CMDTypeOidGPDB>::DebugPrint(os,this);
}

#endif // GPOS_DEBUG

// EOF

