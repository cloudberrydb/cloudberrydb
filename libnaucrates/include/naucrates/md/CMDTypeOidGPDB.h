//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDTypeOidGPDB.h
//
//	@doc:
//		Class for representing OID types in GPDB
//---------------------------------------------------------------------------

#ifndef GPMD_CMDTypeOidGPDB_H
#define GPMD_CMDTypeOidGPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDTypeOid.h"

#define GPDB_OID_OID OID(26)
#define GPDB_OID_LENGTH 4
#define GPDB_OID_EQ_OP OID(607)
#define GPDB_OID_NEQ_OP OID(608)
#define GPDB_OID_LT_OP OID(609)
#define GPDB_OID_LEQ_OP OID(611)
#define GPDB_OID_GT_OP OID(610)
#define GPDB_OID_GEQ_OP OID(612)
#define GPDB_OID_EQ_FUNC OID(184)
#define GPDB_OID_ARRAY_TYPE OID(1028)
#define GPDB_OID_COMP_OP OID(356)
#define GPDB_OID_HASH_OP OID(0)

#define GPDB_OID_AGG_MIN OID(2118)
#define GPDB_OID_AGG_MAX OID(2134)
#define GPDB_OID_AGG_AVG OID(0)
#define GPDB_OID_AGG_SUM OID(0)
#define GPDB_OID_AGG_COUNT OID(2147)

// fwd decl
namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpnaucrates
{
	class IDatumOid;
}

namespace gpmd
{

	using namespace gpos;
	using namespace gpnaucrates;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDTypeOidGPDB
	//
	//	@doc:
	//		Class for representing OID types in GPDB
	//
	//---------------------------------------------------------------------------
	class CMDTypeOidGPDB : public IMDTypeOid
	{
		private:

			// memory pool
			IMemoryPool *m_pmp;

			// type id
			IMDId *m_pmdid;

			// mdids of different comparison operators
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

			// type name and type
			static CWStringConst m_str;
			static CMDName m_mdname;

			// a null datum of this type (used for statistics comparison)
			IDatum *m_pdatumNull;

			// private copy ctor
			CMDTypeOidGPDB(const CMDTypeOidGPDB &);

		public:
			// ctor/dtor
			explicit
			CMDTypeOidGPDB(IMemoryPool *pmp);

			virtual
			~CMDTypeOidGPDB();

			// factory method for creating OID datums
			virtual
			IDatumOid *PdatumOid(IMemoryPool *pmp, OID oValue, BOOL fNULL) const;

			// accessors
			virtual
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}

			virtual
			IMDId *Pmdid() const;

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
				return GPDB_OID_LENGTH;
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

#endif // !GPMD_CMDTypeOidGPDB_H

// EOF
