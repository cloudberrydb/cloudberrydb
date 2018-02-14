//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDTypeGenericGPDB.h
//
//	@doc:
//		Class representing GPDB generic types
//---------------------------------------------------------------------------

#ifndef GPMD_CMDTypeGenericGPDB_H
#define GPMD_CMDTypeGenericGPDB_H

#include "gpos/base.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/IMDTypeGeneric.h"
#include "naucrates/md/CMDIdGPDB.h"

// fwd decl
namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpmd
{
	using namespace gpos;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDTypeGenericGPDB
	//
	//	@doc:
	//		Class representing GPDB generic types
	//
	//---------------------------------------------------------------------------
	class CMDTypeGenericGPDB : public IMDTypeGeneric
	{		
		private:
			// memory pool
			IMemoryPool *m_pmp;
			
			// DXL for object
			const CWStringDynamic *m_pstr;
			
			// metadata id
			IMDId *m_pmdid;
			
			// type name
			CMDName *m_pmdname;
			
			// can type be redistributed
			BOOL m_fRedistributable;
			
			// is this a fixed-length type
			BOOL m_fFixedLength;
			
			// type length in number of bytes for fixed-length types, 0 otherwise
			ULONG m_ulLength;
			
			// is type passed by value or by reference
			BOOL m_fByValue;
			
			// id of equality operator for type
			IMDId *m_pmdidOpEq;
			
			// id of inequality operator for type
			IMDId *m_pmdidOpNeq;

			// id of less than operator for type
			IMDId *m_pmdidOpLT;
			
			// id of less than equals operator for type
			IMDId *m_pmdidOpLEq;

			// id of greater than operator for type
			IMDId *m_pmdidOpGT;
			
			// id of greater than equals operator for type
			IMDId *m_pmdidOpGEq;

			// id of comparison operator for type used in btree lookups
			IMDId *m_pmdidOpComp;
			
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

			// is type hashable
			BOOL m_fHashable;
		
			// is type composite
			BOOL m_fComposite;

			// id of the relation corresponding to a composite type
			IMDId *m_pmdidBaseRelation;

			// id of array type for type
			IMDId *m_pmdidTypeArray;
			
			// GPDB specific length
			INT m_iLength;

			// a null datum of this type (used for statistics comparison)
			IDatum *m_pdatumNull;

			// private copy ctor
			CMDTypeGenericGPDB(const CMDTypeGenericGPDB &);
			
		public:
			// ctor
			CMDTypeGenericGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdname,
				BOOL fRedistributable,
				BOOL fFixedLength,
				ULONG ulLength, 
				BOOL fByValue,
				IMDId *pmdidOpEq,
				IMDId *pmdidOpNEq,
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
				);
			
			// dtor
			virtual 
			~CMDTypeGenericGPDB();
			
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
			
			virtual
			BOOL FRedistributable() const
			{
				return m_fRedistributable;
			}
			
			virtual
			BOOL FFixedLength() const
			{
				return m_fFixedLength;
			}
			
			// is type composite
			virtual
			BOOL FComposite() const
			{
				return m_fComposite;
			}

			virtual
			ULONG UlLength () const
			{
				return m_ulLength;
			}
			
			virtual
			BOOL FByValue() const
			{
				return m_fByValue;
			}
			
			// id of specified comparison operator type
			virtual 
			IMDId *PmdidCmp(ECmpType ecmpt) const;

			// id of specified specified aggregate type
			virtual 
			IMDId *PmdidAgg(EAggType eagg) const;

			virtual 
			const IMDId *PmdidOpComp() const
			{
				return m_pmdidOpComp;
			}
			
			// is type hashable
			virtual 
			BOOL FHashable() const
			{
				return m_fHashable;
			}
			
			// id of the relation corresponding to a composite type
			virtual 
			IMDId *PmdidBaseRelation() const
			{
				return m_pmdidBaseRelation;
			}

			virtual
			IMDId *PmdidTypeArray() const
			{
				return m_pmdidTypeArray;
			}
			
			// serialize object in DXL format
			virtual 
			void Serialize(gpdxl::CXMLSerializer *pxmlser) const;

			// factory method for generating generic datum from CDXLScalarConstValue
			virtual 
			IDatum* Pdatum(const CDXLScalarConstValue *pdxlop) const;

			// create typed datum from DXL datum
			virtual
			IDatum *Pdatum(IMemoryPool *pmp, const CDXLDatum *pdxldatum) const;

			// return the GPDB length
			INT
			ILength() const
			{
				return m_iLength;
			}

			// return the null constant for this type
			virtual
			IDatum *PdatumNull() const
			{
				return m_pdatumNull;
			}

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

			// is type an ambiguous one? e.g., AnyElement in GPDB
			virtual
			BOOL FAmbiguous() const;

			// create a dxl datum
			static
			CDXLDatum *Pdxldatum
						(
						IMemoryPool *pmp,
						IMDId *pmdid,
						INT iTypeModifier,
						BOOL fByVal,
						BOOL fNull,
						BYTE *pba,
						ULONG ulLength,
						LINT lValue,
						CDouble dValue
						);

			// create a dxl datum of types having double mapping
			static
			CDXLDatum *PdxldatumStatsDoubleMappable
						(
						IMemoryPool *pmp,
						IMDId *pmdid,
						INT iTypeModifier,
						BOOL fByValue,
						BOOL fNull,
						BYTE *pba,
						ULONG ulLength,
						LINT lValue,
						CDouble dValue
						);

			// create a dxl datum of types having lint mapping
			static
			CDXLDatum *PdxldatumStatsLintMappable
						(
						IMemoryPool *pmp,
						IMDId *pmdid,
						INT iTypeModifier,
						BOOL fByValue,
						BOOL fNull,
						BYTE *pba,
						ULONG ulLength,
						LINT lValue,
						CDouble dValue
						);

			// does a datum of this type need bytea to Lint mapping for statistics computation
			static
			BOOL FHasByteLintMapping(const IMDId *pmdid);

			// does a datum of this type need bytea to double mapping for statistics computation
			static
			BOOL FHasByteDoubleMapping(const IMDId *pmdid);

			// is this a time-related type
			static
			BOOL FTimeRelatedType(const IMDId *pmdid);

			// is this a network-related type
			static
			BOOL FNetworkRelatedType(const IMDId *pmdid);

	};
}

#endif // !GPMD_CMDTypeGenericGPDB_H

// EOF
