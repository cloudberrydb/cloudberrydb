//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDTypeInt8GPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific int8 type
//		in the MD cache
//
//	@owner: 
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDTypeInt8GPDB.h"
#include "naucrates/md/CGPDBTypeHelper.h"

#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/dxl/operators/CDXLDatumInt8.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "naucrates/base/CDatumInt8GPDB.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpnaucrates;

// static member initialization
CWStringConst
CMDTypeInt8GPDB::m_str = CWStringConst(GPOS_WSZ_LIT("Int8"));
CMDName
CMDTypeInt8GPDB::m_mdname(&m_str);

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::CMDTypeInt8GPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDTypeInt8GPDB::CMDTypeInt8GPDB
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{
	m_pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_OID);
	m_pmdidOpEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_EQ_OP);
	m_pmdidOpNeq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_NEQ_OP);
	m_pmdidOpLT = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_LT_OP);
	m_pmdidOpLEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_LEQ_OP);
	m_pmdidOpGT = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_GT_OP);
	m_pmdidOpGEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_GEQ_OP);
	m_pmdidOpComp = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_COMP_OP);
	m_pmdidTypeArray = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_ARRAY_TYPE);
	
	m_pmdidMin = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_AGG_MIN);
	m_pmdidMax = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_AGG_MAX);
	m_pmdidAvg = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_AGG_AVG);
	m_pmdidSum = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_AGG_SUM);
	m_pmdidCount = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT8_AGG_COUNT);

	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);

	GPOS_ASSERT(GPDB_INT8_OID == CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId());
	m_pmdid->AddRef();
	m_pdatumNull = GPOS_NEW(pmp) CDatumInt8GPDB(m_pmdid, 1 /* fVal */, true /* fNull */);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::~CMDTypeInt8GPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDTypeInt8GPDB::~CMDTypeInt8GPDB()
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
//		CMDTypeInt8GPDB::Pdatum
//
//	@doc:
//		Factory function for creating Int8 datums
//
//---------------------------------------------------------------------------
IDatumInt8 *
CMDTypeInt8GPDB::PdatumInt8
	(
	IMemoryPool *pmp,
	LINT lValue,
	BOOL fNULL
	)
	const
{
	return GPOS_NEW(pmp) CDatumInt8GPDB(m_pmdid->Sysid(), lValue, fNULL);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt8GPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::Mdname
//
//	@doc:
//		Returns the name of this type
//
//---------------------------------------------------------------------------
CMDName
CMDTypeInt8GPDB::Mdname() const
{
	return m_mdname;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::PmdidCmp
//
//	@doc:
//		Return mdid of specified comparison operator type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt8GPDB::PmdidCmp
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
//		CMDTypeInt8GPDB::PmdidAgg
//
//	@doc:
//		Return mdid of specified aggregate type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt8GPDB::PmdidAgg
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
//		CMDTypeInt8GPDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDTypeInt8GPDB::Serialize
	(
	CXMLSerializer *pxmlser
	)
	const
{
	CGPDBTypeHelper<CMDTypeInt8GPDB>::Serialize(pxmlser, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::Pdatum
//
//	@doc:
//		Transformation method for generating Int8 datum from CDXLScalarConstValue
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt8GPDB::Pdatum
	(
	const CDXLScalarConstValue *pdxlop
	)
	const
{
	CDXLDatumInt8 *pdxldatum = CDXLDatumInt8::PdxldatumConvert(const_cast<CDXLDatum*>(pdxlop->Pdxldatum()));
	GPOS_ASSERT(pdxldatum->FByValue());

	return GPOS_NEW(m_pmp) CDatumInt8GPDB(m_pmdid->Sysid(), pdxldatum->LValue(), pdxldatum->FNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::Pdatum
//
//	@doc:
//		Construct an int8 datum from a DXL datum
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt8GPDB::Pdatum
	(
	IMemoryPool *pmp,
	const CDXLDatum *pdxldatum
	)
	const
{
	CDXLDatumInt8 *pdxldatumint8 = CDXLDatumInt8::PdxldatumConvert(const_cast<CDXLDatum *>(pdxldatum));
	GPOS_ASSERT(pdxldatumint8->FByValue());

	LINT lVal = pdxldatumint8->LValue();
	BOOL fNull = pdxldatumint8->FNull();

	return GPOS_NEW(pmp) CDatumInt8GPDB(m_pmdid->Sysid(), lVal, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::Pdxldatum
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt8GPDB::Pdxldatum
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	CDatumInt8GPDB *pdatumint8 = dynamic_cast<CDatumInt8GPDB*>(pdatum);

	m_pmdid->AddRef();
	return GPOS_NEW(pmp) CDXLDatumInt8(pmp, m_pmdid, pdatumint8->FNull(), pdatumint8->LValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::PdxlopScConst
//
//	@doc:
// 		Generate a dxl scalar constant from a datum
//
//---------------------------------------------------------------------------
CDXLScalarConstValue *
CMDTypeInt8GPDB::PdxlopScConst
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	CDatumInt8GPDB *pdatumint8gpdb = dynamic_cast<CDatumInt8GPDB *>(pdatum);

	m_pmdid->AddRef();
	CDXLDatumInt8 *pdxldatum = GPOS_NEW(pmp) CDXLDatumInt8(pmp, m_pmdid, pdatumint8gpdb->FNull(), pdatumint8gpdb->LValue());

	return GPOS_NEW(pmp) CDXLScalarConstValue(pmp, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::PdxldatumNull
//
//	@doc:
// 		Generate dxl null datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt8GPDB::PdxldatumNull
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();

	return GPOS_NEW(pmp) CDXLDatumInt8(pmp, m_pmdid, true /*fNull*/, 1);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDTypeInt8GPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	CGPDBTypeHelper<CMDTypeInt8GPDB>::DebugPrint(os,this);
}

#endif // GPOS_DEBUG

// EOF

