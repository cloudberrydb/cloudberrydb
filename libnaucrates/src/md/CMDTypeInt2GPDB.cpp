//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CMDTypeInt2GPDB.cpp
//
//	@doc:
//		Implementation of the class for representing GPDB-specific int2 type in the
//		MD cache
//---------------------------------------------------------------------------

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/CMDTypeInt2GPDB.h"
#include "naucrates/md/CGPDBTypeHelper.h"

#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/dxl/operators/CDXLDatumInt2.h"
#include "naucrates/dxl/CDXLUtils.h"

#include "naucrates/base/CDatumInt2GPDB.h"

using namespace gpdxl;
using namespace gpmd;
using namespace gpnaucrates;

// static member initialization
CWStringConst
CMDTypeInt2GPDB::m_str = CWStringConst(GPOS_WSZ_LIT("int2"));
CMDName
CMDTypeInt2GPDB::m_mdname(&m_str);

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::CMDTypeInt2GPDB
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CMDTypeInt2GPDB::CMDTypeInt2GPDB
	(
	IMemoryPool *pmp
	)
	:
	m_pmp(pmp)
{
	m_pmdid = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_OID);
	m_pmdidOpEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_EQ_OP);
	m_pmdidOpNeq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_NEQ_OP);
	m_pmdidOpLT = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_LT_OP);
	m_pmdidOpLEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_LEQ_OP);
	m_pmdidOpGT = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_GT_OP);
	m_pmdidOpGEq = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_GEQ_OP);
	m_pmdidOpComp = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_COMP_OP);
	m_pmdidTypeArray = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_ARRAY_TYPE);
	m_pmdidMin = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_AGG_MIN);
	m_pmdidMax = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_AGG_MAX);
	m_pmdidAvg = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_AGG_AVG);
	m_pmdidSum = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_AGG_SUM);
	m_pmdidCount = GPOS_NEW(pmp) CMDIdGPDB(GPDB_INT2_AGG_COUNT);

	m_pstr = CDXLUtils::PstrSerializeMDObj(m_pmp, this, false /*fSerializeHeader*/, false /*fIndent*/);

	GPOS_ASSERT(GPDB_INT2_OID == CMDIdGPDB::PmdidConvert(m_pmdid)->OidObjectId());
	m_pmdid->AddRef();
	m_pdatumNull = GPOS_NEW(pmp) CDatumInt2GPDB(m_pmdid, 1 /* fVal */, true /* fNull */);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::~CMDTypeInt2GPDB
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CMDTypeInt2GPDB::~CMDTypeInt2GPDB()
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
//		CMDTypeInt2GPDB::Pdatum
//
//	@doc:
//		Factory function for creating INT2 datums
//
//---------------------------------------------------------------------------
IDatumInt2 *
CMDTypeInt2GPDB::PdatumInt2
	(
	IMemoryPool *pmp, 
	SINT sValue,
	BOOL fNULL
	)
	const
{
	return GPOS_NEW(pmp) CDatumInt2GPDB(m_pmdid->Sysid(), sValue, fNULL);
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::Pmdid
//
//	@doc:
//		Returns the metadata id of this type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt2GPDB::Pmdid() const
{
	return m_pmdid;
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::Mdname
//
//	@doc:
//		Returns the name of this type
//
//---------------------------------------------------------------------------
CMDName
CMDTypeInt2GPDB::Mdname() const
{
	return CMDTypeInt2GPDB::m_mdname;
}


//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::PmdidCmp
//
//	@doc:
//		Return mdid of specified comparison operator type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt2GPDB::PmdidCmp
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
//		CMDTypeInt2GPDB::PmdidAgg
//
//	@doc:
//		Return mdid of specified aggregate type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt2GPDB::PmdidAgg
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
//		CMDTypeInt2GPDB::Serialize
//
//	@doc:
//		Serialize relation metadata in DXL format
//
//---------------------------------------------------------------------------
void
CMDTypeInt2GPDB::Serialize
	(
	CXMLSerializer *pxmlser
	) 
	const
{
	CGPDBTypeHelper<CMDTypeInt2GPDB>::Serialize(pxmlser, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::Pdatum
//
//	@doc:
//		Transformation method for generating int2 datum from CDXLScalarConstValue
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt2GPDB::Pdatum
	(
	const CDXLScalarConstValue *pdxlop
	)
	const
{
	CDXLDatumInt2 *pdxldatum = CDXLDatumInt2::PdxldatumConvert(const_cast<CDXLDatum*>(pdxlop->Pdxldatum()));
	GPOS_ASSERT(pdxldatum->FByValue());

	return GPOS_NEW(m_pmp) CDatumInt2GPDB(m_pmdid->Sysid(), pdxldatum->SValue(), pdxldatum->FNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::Pdatum
//
//	@doc:
//		Construct an int2 datum from a DXL datum
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt2GPDB::Pdatum
	(
	IMemoryPool *pmp,
	const CDXLDatum *pdxldatum
	)
	const
{
	CDXLDatumInt2 *pdxldatumint2 = CDXLDatumInt2::PdxldatumConvert(const_cast<CDXLDatum *>(pdxldatum));
	GPOS_ASSERT(pdxldatumint2->FByValue());
	SINT sVal = pdxldatumint2->SValue();
	BOOL fNull = pdxldatumint2->FNull();

	return GPOS_NEW(pmp) CDatumInt2GPDB(m_pmdid->Sysid(), sVal, fNull);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::Pdxldatum
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt2GPDB::Pdxldatum
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	m_pmdid->AddRef();
	CDatumInt2GPDB *pdatumint2 = dynamic_cast<CDatumInt2GPDB*>(pdatum);

	return GPOS_NEW(pmp) CDXLDatumInt2(pmp, m_pmdid, pdatumint2->FNull(), pdatumint2->SValue());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::PdxlopScConst
//
//	@doc:
// 		Generate a dxl scalar constant from a datum
//
//---------------------------------------------------------------------------
CDXLScalarConstValue *
CMDTypeInt2GPDB::PdxlopScConst
	(
	IMemoryPool *pmp,
	IDatum *pdatum
	)
	const
{
	CDatumInt2GPDB *pdatumint2gpdb = dynamic_cast<CDatumInt2GPDB *>(pdatum);

	m_pmdid->AddRef();
	CDXLDatumInt2 *pdxldatum = GPOS_NEW(pmp) CDXLDatumInt2(pmp, m_pmdid, pdatumint2gpdb->FNull(), pdatumint2gpdb->SValue());

	return GPOS_NEW(pmp) CDXLScalarConstValue(pmp, pdxldatum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::PdxldatumNull
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt2GPDB::PdxldatumNull
	(
	IMemoryPool *pmp
	)
	const
{
	m_pmdid->AddRef();

	return GPOS_NEW(pmp) CDXLDatumInt2(pmp, m_pmdid, true /*fNull*/, 1 /* a dummy value */);
}

#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt2GPDB::DebugPrint
//
//	@doc:
//		Prints a metadata cache relation to the provided output
//
//---------------------------------------------------------------------------
void
CMDTypeInt2GPDB::DebugPrint
	(
	IOstream &os
	)
	const
{
	CGPDBTypeHelper<CMDTypeInt2GPDB>::DebugPrint(os,this);
}

#endif // GPOS_DEBUG

// EOF

