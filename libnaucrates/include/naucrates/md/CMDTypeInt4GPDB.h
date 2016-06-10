//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDTypeInt4GPDB.h
//
//	@doc:
//		Class for representing INT4 types in GPDB
//---------------------------------------------------------------------------



#ifndef GPMD_CMDTypeInt4GPDB_H
#define GPMD_CMDTypeInt4GPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDTypeInt4.h"

#define GPDB_INT4_OID OID(23)
#define GPDB_INT4_LENGTH 4
#define GPDB_INT4_EQ_OP OID(96)
#define GPDB_INT4_NEQ_OP OID(518)
#define GPDB_INT4_LT_OP OID(97)
#define GPDB_INT4_LEQ_OP OID(523)
#define GPDB_INT4_GT_OP OID(521)
#define GPDB_INT4_GEQ_OP OID(525)
#define GPDB_INT4_COMP_OP OID(351)
#define GPDB_INT4_EQ_FUNC OID(65)
#define GPDB_INT4_ARRAY_TYPE OID(1007)

#define GPDB_INT4_AGG_MIN OID(2132)
#define GPDB_INT4_AGG_MAX OID(2116)
#define GPDB_INT4_AGG_AVG OID(2101)
#define GPDB_INT4_AGG_SUM OID(2108)
#define GPDB_INT4_AGG_COUNT OID(2147)

// fwd decl
namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpnaucrates
{
	class IDatumInt4;
}

namespace gpmd
{

	using namespace gpos;
	using namespace gpnaucrates;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDTypeInt4GPDB
	//
	//	@doc:
	//		Class for representing INT4 types in GPDB
	//
	//---------------------------------------------------------------------------
	class CMDTypeInt4GPDB : public IMDTypeInt4
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
			
			// type name and type
			static CWStringConst m_str;
			static CMDName m_mdname;

			// a null datum of this type (used for statistics comparison)
			IDatum *m_pdatumNull;

			// private copy ctor
			CMDTypeInt4GPDB(const CMDTypeInt4GPDB &);
	
		public:
			// ctor
			explicit 
			CMDTypeInt4GPDB(IMemoryPool *pmp);
	
			//dtor
			virtual
			~CMDTypeInt4GPDB();
	
			// factory method for creating INT4 datums
			virtual
			IDatumInt4 *PdatumInt4(IMemoryPool *pmp, INT iValue, BOOL fNULL) const;
	
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
				return GPDB_INT4_LENGTH;
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

#endif // !GPMD_CMDTypeInt4GPDB_H

// EOF
