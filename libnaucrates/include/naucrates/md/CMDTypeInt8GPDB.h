//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDTypeInt8GPDB.h
//
//	@doc:
//		Class for representing Int8 types in GPDB
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPMD_CMDTypeInt8GPDB_H
#define GPMD_CMDTypeInt8GPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDTypeInt8.h"

#define GPDB_INT8_OID OID(20)
#define GPDB_INT8_LENGTH 8
#define GPDB_INT8_EQ_OP OID(410)
#define GPDB_INT8_NEQ_OP OID(411)
#define GPDB_INT8_LT_OP OID(412)
#define GPDB_INT8_LEQ_OP OID(414)
#define GPDB_INT8_GT_OP OID(413)
#define GPDB_INT8_GEQ_OP OID(415)
#define GPDB_INT8_COMP_OP OID(351)
#define GPDB_INT8_ARRAY_TYPE OID(1016)

#define GPDB_INT8_AGG_MIN OID(2131)
#define GPDB_INT8_AGG_MAX OID(2115)
#define GPDB_INT8_AGG_AVG OID(2100)
#define GPDB_INT8_AGG_SUM OID(2107)
#define GPDB_INT8_AGG_COUNT OID(2147)

// fwd decl
namespace gpdxl
{
	class CXMLSerializer;
}


namespace gpnaucrates
{
	class IDatumInt8;
}


namespace gpmd
{


	using namespace gpos;
	using namespace gpnaucrates;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDTypeInt8GPDB
	//
	//	@doc:
	//		Class for representing Int8 types in GPDB
	//
	//---------------------------------------------------------------------------
	class CMDTypeInt8GPDB : public IMDTypeInt8
	{
	private:

		// memory pool
		IMemoryPool *m_pmp;
		
		// type id
		IMDId *m_pmdid;
		
		// mdids of different operators
		IMDId *m_pmdidOpEq;
		IMDId *m_pmdidOpNeq;
		IMDId *m_pmdidOpLT;
		IMDId *m_pmdidOpLEq;
		IMDId *m_pmdidOpGT;
		IMDId *m_pmdidOpGEq;
		IMDId *m_pmdidOpComp;
		IMDId *m_pmdidTypeArray;
		
		// min aggregate
		IMDId *m_pmdidMin;
		
		// max aggregate
		IMDId *m_pmdidMax;
		
		// avg aggregate
		IMDId *m_pmdidAvg;
		
		// sum aggregate
		IMDId *m_pmdidSum;
		
		// count aggregate
		IMDId *m_pmdidCount;

		// DXL for object
		const CWStringDynamic *m_pstr;

		// type name
		static CWStringConst m_str;
		static CMDName m_mdname;

		// a null datum of this type (used for statistics comparison)
		IDatum *m_pdatumNull;

		// private copy ctor
		CMDTypeInt8GPDB(const CMDTypeInt8GPDB &);

	public:
		// ctor/dtor
		explicit CMDTypeInt8GPDB(IMemoryPool *pmp);

		virtual 
		~CMDTypeInt8GPDB();

		// factory method for creating Int8 datums
		virtual
		IDatumInt8 *PdatumInt8(IMemoryPool *pmp, LINT lValue, BOOL fNULL) const;

		// accessors
		virtual 
		const CWStringDynamic *Pstr() const
		{
			return m_pstr;
		}

		// type id
		virtual 
		IMDId *Pmdid() const;
		
		// type name
		virtual 
		CMDName Mdname() const;

		// id of specified comparison operator type
		virtual 
		IMDId *PmdidCmp(ECmpType ecmpt) const;

		// id of specified specified aggregate type
		virtual 
		IMDId *PmdidAgg(EAggType eagg) const;

		virtual
		BOOL FRedistributable() const
		{
			return true;
		}

		virtual
		BOOL FFixedLength() const
		{
			return true;
		}

		// is type composite
		virtual
		BOOL FComposite() const
		{
			return false;
		}

		virtual
		ULONG UlLength() const
		{
			return GPDB_INT8_LENGTH;
		}

		virtual
		BOOL FByValue() const
		{
			return true;
		}

		virtual 
		const IMDId *PmdidOpComp() const
		{
			return m_pmdidOpComp;
		}
		
		// is type hashable
		virtual 
		BOOL FHashable() const
		{
			return true;
		}

		virtual 
		IMDId *PmdidTypeArray() const
		{
			return m_pmdidTypeArray;
		}
		
		// id of the relation corresponding to a composite type
		virtual
		IMDId *PmdidBaseRelation() const
		{
			return NULL;
		}

		// serialize object in DXL format
		virtual 
		void Serialize(gpdxl::CXMLSerializer *pxmlser) const;

		// return the null constant for this type
		virtual
		IDatum *PdatumNull() const
		{
			return m_pdatumNull;
		}

		// transformation method for generating datum from CDXLScalarConstValue
		virtual 
		IDatum* Pdatum(const CDXLScalarConstValue *pdxlop) const;

		// create typed datum from DXL datum
		virtual
		IDatum *Pdatum(IMemoryPool *pmp, const CDXLDatum *pdxldatum) const;

		// generate the DXL datum from IDatum
		virtual
		CDXLDatum* Pdxldatum(IMemoryPool *pmp, IDatum *pdatum) const;

		// generate the DXL datum representing null value
		virtual
		CDXLDatum* PdxldatumNull(IMemoryPool *pmp) const;

		// generate the DXL scalar constant from IDatum
		virtual
		CDXLScalarConstValue* PdxlopScConst(IMemoryPool *pmp, IDatum *pdatum) const;

#ifdef GPOS_DEBUG
		// debug print of the type in the provided stream
		virtual 
		void DebugPrint(IOstream &os) const;
#endif

	};
}

#endif // !GPMD_CMDTypeInt8GPDB_H

// EOF
