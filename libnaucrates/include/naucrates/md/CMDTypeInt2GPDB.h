//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CMDTypeInt2GPDB.h
//
//	@doc:
//		Class for representing INT2 types in GPDB
//---------------------------------------------------------------------------



#ifndef GPMD_CMDTypeInt2GPDB_H
#define GPMD_CMDTypeInt2GPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDTypeInt2.h"

#define GPDB_INT2_OID OID(21)
#define GPDB_INT2_LENGTH 2
#define GPDB_INT2_EQ_OP OID(94)
#define GPDB_INT2_NEQ_OP OID(519)
#define GPDB_INT2_LT_OP OID(95)
#define GPDB_INT2_LEQ_OP OID(522)
#define GPDB_INT2_GT_OP OID(520)
#define GPDB_INT2_GEQ_OP OID(524)
#define GPDB_INT2_COMP_OP OID(350)
#define GPDB_INT2_EQ_FUNC OID(63)
#define GPDB_INT2_ARRAY_TYPE OID(1005)

#define GPDB_INT2_AGG_MIN OID(2133)
#define GPDB_INT2_AGG_MAX OID(2117)
#define GPDB_INT2_AGG_AVG OID(2102)
#define GPDB_INT2_AGG_SUM OID(2109)
#define GPDB_INT2_AGG_COUNT OID(2147)

// fwd decl
namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpnaucrates
{
	class IDatumInt2;
}

namespace gpmd
{

	using namespace gpos;
	using namespace gpnaucrates;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDTypeInt2GPDB
	//
	//	@doc:
	//		Class for representing INT2 type in GPDB
	//
	//---------------------------------------------------------------------------
	class CMDTypeInt2GPDB : public IMDTypeInt2
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
			CMDTypeInt2GPDB(const CMDTypeInt2GPDB &);
	
		public:
			// ctor
			explicit 
			CMDTypeInt2GPDB(IMemoryPool *pmp);
	
			// dtor
			virtual
			~CMDTypeInt2GPDB();
	
			// factory method for creating INT2 datums
			virtual
			IDatumInt2 *PdatumInt2(IMemoryPool *pmp, SINT sValue, BOOL fNULL) const;
	
			// accessors
			virtual 
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}
			
			// accessor of metadata id
			virtual 
			IMDId *Pmdid() const;
			
			// accessor of type name
			virtual 
			CMDName Mdname() const;
			
			// id of specified comparison operator type
			virtual 
			IMDId *PmdidCmp(ECmpType ecmpt) const;

			// id of specified aggregate type
			virtual 
			IMDId *PmdidAgg(EAggType eagg) const;

			// is type redistributable
			virtual
			BOOL FRedistributable() const
			{
				return true;
			}
			
			// is type has fixed length
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

			// size of type
			virtual
			ULONG UlLength() const
			{
				return GPDB_INT2_LENGTH;
			}
			
			// is type passed by value
			virtual
			BOOL FByValue() const
			{
				return true;
			}
			
			// metadata id of b-tree lookup operator
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
			
			// metadata id of array type
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

#endif // !GPMD_CMDTypeInt2GPDB_H

// EOF
