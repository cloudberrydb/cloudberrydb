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
	IMemoryPool *mp
	)
	:
	m_mp(mp)
{
	m_mdid = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_OID);
	m_mdid_op_eq = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_EQ_OP);
	m_mdid_op_neq = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_NEQ_OP);
	m_mdid_op_lt = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_LT_OP);
	m_mdid_op_leq = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_LEQ_OP);
	m_mdid_op_gt = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_GT_OP);
	m_mdid_op_geq = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_GEQ_OP);
	m_mdid_op_cmp = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_COMP_OP);
	m_mdid_type_array = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_ARRAY_TYPE);
	
	m_mdid_min = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_AGG_MIN);
	m_mdid_max = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_AGG_MAX);
	m_mdid_avg = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_AGG_AVG);
	m_mdid_sum = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_AGG_SUM);
	m_mdid_count = GPOS_NEW(mp) CMDIdGPDB(GPDB_INT8_AGG_COUNT);

	m_dxl_str = CDXLUtils::SerializeMDObj(m_mp, this, false /*fSerializeHeader*/, false /*indentation*/);

	GPOS_ASSERT(GPDB_INT8_OID == CMDIdGPDB::CastMdid(m_mdid)->Oid());
	m_mdid->AddRef();
	m_datum_null = GPOS_NEW(mp) CDatumInt8GPDB(m_mdid, 1 /* value */, true /* is_null */);
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
	m_mdid->Release();
	m_mdid_op_eq->Release();
	m_mdid_op_neq->Release();
	m_mdid_op_lt->Release();
	m_mdid_op_leq->Release();
	m_mdid_op_gt->Release();
	m_mdid_op_geq->Release();
	m_mdid_op_cmp->Release();
	m_mdid_type_array->Release();
	
	m_mdid_min->Release();
	m_mdid_max->Release();
	m_mdid_avg->Release();
	m_mdid_sum->Release();
	m_mdid_count->Release();
	m_datum_null->Release();
	
	GPOS_DELETE(m_dxl_str);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::GetDatum
//
//	@doc:
//		Factory function for creating Int8 datums
//
//---------------------------------------------------------------------------
IDatumInt8 *
CMDTypeInt8GPDB::CreateInt8Datum
	(
	IMemoryPool *mp,
	LINT value,
	BOOL is_null
	)
	const
{
	return GPOS_NEW(mp) CDatumInt8GPDB(m_mdid->Sysid(), value, is_null);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::MDId
//
//	@doc:
//		Returns the metadata id of this type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt8GPDB::MDId() const
{
	return m_mdid;
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
//		CMDTypeInt8GPDB::GetMdidForCmpType
//
//	@doc:
//		Return mdid of specified comparison operator type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt8GPDB::GetMdidForCmpType
	(
	ECmpType cmp_type
	) 
	const
{
	switch (cmp_type)
	{
		case EcmptEq:
			return m_mdid_op_eq;
		case EcmptNEq:
			return m_mdid_op_neq;
		case EcmptL:
			return m_mdid_op_lt;
		case EcmptLEq: 
			return m_mdid_op_leq;
		case EcmptG:
			return m_mdid_op_gt;
		case EcmptGEq:
			return m_mdid_op_geq;
		default:
			GPOS_ASSERT(!"Invalid operator type");
			return NULL;
	}
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::GetMdidForAggType
//
//	@doc:
//		Return mdid of specified aggregate type
//
//---------------------------------------------------------------------------
IMDId *
CMDTypeInt8GPDB::GetMdidForAggType
	(
	EAggType agg_type
	) 
	const
{
	switch (agg_type)
	{
		case EaggMin:
			return m_mdid_min;
		case EaggMax:
			return m_mdid_max;
		case EaggAvg:
			return m_mdid_avg;
		case EaggSum:
			return m_mdid_sum;
		case EaggCount:
			return m_mdid_count;
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
	CXMLSerializer *xml_serializer
	)
	const
{
	CGPDBTypeHelper<CMDTypeInt8GPDB>::Serialize(xml_serializer, this);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::GetDatumForDXLConstVal
//
//	@doc:
//		Transformation method for generating Int8 datum from CDXLScalarConstValue
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt8GPDB::GetDatumForDXLConstVal
	(
	const CDXLScalarConstValue *dxl_op
	)
	const
{
	CDXLDatumInt8 *dxl_datum = CDXLDatumInt8::Cast(const_cast<CDXLDatum *>(dxl_op->GetDatumVal()));
	GPOS_ASSERT(dxl_datum->IsPassedByValue());

	return GPOS_NEW(m_mp) CDatumInt8GPDB(m_mdid->Sysid(), dxl_datum->Value(), dxl_datum->IsNull());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::GetDatumForDXLDatum
//
//	@doc:
//		Construct an int8 datum from a DXL datum
//
//---------------------------------------------------------------------------
IDatum*
CMDTypeInt8GPDB::GetDatumForDXLDatum
	(
	IMemoryPool *mp,
	const CDXLDatum *dxl_datum
	)
	const
{
	CDXLDatumInt8 *int8_dxl_datum = CDXLDatumInt8::Cast(const_cast<CDXLDatum *>(dxl_datum));
	GPOS_ASSERT(int8_dxl_datum->IsPassedByValue());

	LINT val = int8_dxl_datum->Value();
	BOOL is_null = int8_dxl_datum->IsNull();

	return GPOS_NEW(mp) CDatumInt8GPDB(m_mdid->Sysid(), val, is_null);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::GetDatumVal
//
//	@doc:
// 		Generate dxl datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt8GPDB::GetDatumVal
	(
	IMemoryPool *mp,
	IDatum *datum
	)
	const
{
	CDatumInt8GPDB *int8_datum = dynamic_cast<CDatumInt8GPDB*>(datum);

	m_mdid->AddRef();
	return GPOS_NEW(mp) CDXLDatumInt8(mp, m_mdid, int8_datum->IsNull(), int8_datum->Value());
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::GetDXLOpScConst
//
//	@doc:
// 		Generate a dxl scalar constant from a datum
//
//---------------------------------------------------------------------------
CDXLScalarConstValue *
CMDTypeInt8GPDB::GetDXLOpScConst
	(
	IMemoryPool *mp,
	IDatum *datum
	)
	const
{
	CDatumInt8GPDB *int8gpdb_datum = dynamic_cast<CDatumInt8GPDB *>(datum);

	m_mdid->AddRef();
	CDXLDatumInt8 *dxl_datum = GPOS_NEW(mp) CDXLDatumInt8(mp, m_mdid, int8gpdb_datum->IsNull(), int8gpdb_datum->Value());

	return GPOS_NEW(mp) CDXLScalarConstValue(mp, dxl_datum);
}

//---------------------------------------------------------------------------
//	@function:
//		CMDTypeInt8GPDB::GetDXLDatumNull
//
//	@doc:
// 		Generate dxl null datum
//
//---------------------------------------------------------------------------
CDXLDatum *
CMDTypeInt8GPDB::GetDXLDatumNull
	(
	IMemoryPool *mp
	)
	const
{
	m_mdid->AddRef();

	return GPOS_NEW(mp) CDXLDatumInt8(mp, m_mdid, true /*is_null*/, 1);
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

