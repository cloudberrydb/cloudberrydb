//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDTypeInt4GPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific int4 type in the
//		MD cache
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDTypeInt4GPDB.h"
#include "naucrates/md/CGPDBTypeHelper.h"

#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/dxl/operators/CDXLDatumInt4.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "naucrates/base/CDatumInt4GPDB.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpnaucrates;

// static member initialization
CWStringConst
CMDTypeInt4GPDB::m_str = CWStringConst(GPOS_WSZ_LIT("int4"));
CMDName
CMDTypeInt4GPDB::m_mdname(&m_str);

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::CMDTypeInt4GPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDTypeInt4GPDB::CMDTypeInt4GPDB
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{
	m_pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_OID);
	m_pmdidOpEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_EQ_OP);
	m_pmdidOpNeq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_NEQ_OP);
	m_pmdidOpLT = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_LT_OP);
	m_pmdidOpLEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_LEQ_OP);
	m_pmdidOpGT = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_GT_OP);
	m_pmdidOpGEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_GEQ_OP);
	m_pmdidOpComp = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_COMP_OP);
	m_pmdidTypeArray = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_ARRAY_TYPE);
	m_pmdidMin = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_AGG_MIN);
	m_pmdidMax = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_AGG_MAX);
	m_pmdidAvg = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_AGG_AVG);
	m_pmdidSum = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_AGG_SUM);
	m_pmdidCount = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT4_AGG_COUNT);

	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);

	GPOS_ASSERT(GPDB_INT4_OID == CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId());
	m_pmdid->AddRef();
	m_pdatumNull = GPOS_NEW(pmp) CDatumInt4GPDB(m_pmdid, 1 /* fVal */, true /* fNull */);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::~CMDTypeInt4GPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDTypeInt4GPDB::~CMDTypeInt4GPDB()
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
//		CMDTypeInt4GPDB::Pdatum
//
//	@doc:
//		Factory function for creating INT4 datums
//
//---------------------------------------------------------------------------
IDatumInt4 *
CMDTypeInt4GPDB::PdatumInt4
	(
	IMemoryPool *pmp, 
	INT iValue, 
	BOOL fNULL
	)
	const
{
	return GPOS_NEW(pmp) CDatumInt4GPDB(m_pmdid->Sysid(), iValue, fNULL);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt4GPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::Mdname
//
//	@doc:
//		Returns the name of this type
//
//---------------------------------------------------------------------------
CMDName
CMDTypeInt4GPDB::Mdname() const
{
	return CMDTypeInt4GPDB::m_mdname;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::PmdidCmp
//
//	@doc:
//		Return mdid of specified comparison operator type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt4GPDB::PmdidCmp
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
//		CMDTypeInt4GPDB::PmdidAgg
//
//	@doc:
//		Return mdid of specified aggregate type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt4GPDB::PmdidAgg
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
//		CMDTypeInt4GPDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDTypeInt4GPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	CGPDBTypeHelper<CMDTypeInt4GPDB>::Serialize(pxmlser, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::Pdatum
//
//	@doc:
//		Transformation method for generating int4 datum from CDXLScalarConstValue
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt4GPDB::Pdatum
	(
	const CDXLScalarConstValue *pdxlop
	)
	const
{
	CDXLDatumInt4 *pdxldatum = CDXLDatumInt4::PdxldatumConvert(const_cast<CDXLDatum*>(pdxlop->Pdxldatum()));
	GPOS_ASSERT(pdxldatum->FByValue());

	return GPOS_NEW(m_pmp) CDatumInt4GPDB(m_pmdid->Sysid(), pdxldatum->IValue(), pdxldatum->FNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::Pdatum
//
//	@doc:
//		Construct an int4 datum from a DXL datum
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt4GPDB::Pdatum
	(
	IMemoryPool *pmp,
	const CDXLDatum *pdxldatum
	)
	const
{
	CDXLDatumInt4 *pdxldatumint4 = CDXLDatumInt4::PdxldatumConvert(const_cast<CDXLDatum *>(pdxldatum));
	GPOS_ASSERT(pdxldatumint4->FByValue());
	INT iVal = pdxldatumint4->IValue();
	BOOL fNull = pdxldatumint4->FNull();

	return GPOS_NEW(pmp) CDatumInt4GPDB(m_pmdid->Sysid(), iVal, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::Pdxldatum
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt4GPDB::Pdxldatum
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	m_pmdid->AddRef();
	CDatumInt4GPDB *pdatumint4 = dynamic_cast<CDatumInt4GPDB*>(pdatum);

	return GPOS_NEW(pmp) CDXLDatumInt4(pmp, m_pmdid, pdatumint4->FNull(), pdatumint4->IValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::PdxlopScConst
//
//	@doc:
// 		Generate a dxl scalar constant from a datum
//
//---------------------------------------------------------------------------
CDXLScalarConstValue *
CMDTypeInt4GPDB::PdxlopScConst
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	CDatumInt4GPDB *pdatumint4gpdb = dynamic_cast<CDatumInt4GPDB *>(pdatum);

	m_pmdid->AddRef();
	CDXLDatumInt4 *pdxldatum = GPOS_NEW(pmp) CDXLDatumInt4(pmp, m_pmdid, pdatumint4gpdb->FNull(), pdatumint4gpdb->IValue());

	return GPOS_NEW(pmp) CDXLScalarConstValue(pmp, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::PdxldatumNull
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt4GPDB::PdxldatumNull
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();

	return GPOS_NEW(pmp) CDXLDatumInt4(pmp, m_pmdid, true /*fNull*/, 1);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt4GPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDTypeInt4GPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	CGPDBTypeHelper<CMDTypeInt4GPDB>::DebugPrint(os,this);
}

#endif // GPOS_DEBUG

// EOF

