//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		IMDType.h
//
//	@doc:
//		Interface for types in the metadata cache
//---------------------------------------------------------------------------



#ifndef GPMD_IMDCacheType_H
#define GPMD_IMDCacheType_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalarConstValue.h"
#include "naucrates/dxl/operators/CDXLDatum.h"
#include "naucrates/md/IMDCacheObject.h"

// fwd decl
namespace gpdxl
{
	class CDXLScalarConstValue;
	class CDXLDatum;
}

namespace gpnaucrates
{
	class IDatum;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpnaucrates;
	using namespace gpdxl;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		IMDType
	//
	//	@doc:
	//		Interface for types in the metadata cache
	//
	//---------------------------------------------------------------------------
	class IMDType : public IMDCacheObject
	{		
		public:
			
			enum ETypeInfo
			{
				EtiInt2,
				EtiInt4,
				EtiInt8,
				EtiBool,
				EtiOid,
				EtiGeneric // should be the last in this enum
			};
			
			// comparison type
			enum ECmpType
			{
				EcmptEq,	// equals
				EcmptNEq,	// not equals
				EcmptL,		// less than
				EcmptLEq,	// less or equal to
				EcmptG,		// greater than
				EcmptGEq,	// greater or equal to
				EcmptIDF,	// is distinct from
				EcmptOther
			};
		
			// aggregate type
			enum EAggType
			{
				EaggMin,
				EaggMax,
				EaggAvg,
				EaggSum,
				EaggCount,
				EaggGeneric
			};
			
			// object type
			virtual
			Emdtype Emdt() const
			{
				return EmdtType;
			}
			
			// md id of cache object
			virtual 
			IMDId *Pmdid() const = 0;
			
			// id of specified specified comparison operator type
			virtual 
			IMDId *PmdidCmp(ECmpType ecmpt) const = 0;
						
			// id of specified specified aggregate type
			virtual 
			IMDId *PmdidAgg(EAggType eagg) const = 0;

			// id of comparison operator for type used in btree lookups
			virtual 
			const IMDId *PmdidOpComp() const = 0;
			
			// id of hash operator for type
			virtual 
			BOOL FHashable() const = 0;
			
			// id of the array type for the type
			virtual 
			IMDId *PmdidTypeArray() const = 0;
			
			// type id
			virtual 
			ETypeInfo Eti() const = 0;

			// transformation function for datums
			virtual 
			IDatum* Pdatum(const CDXLScalarConstValue *pdxlop) const = 0;

			// construct a datum from a DXL datum
			virtual 
			IDatum* Pdatum(IMemoryPool *pmp, const CDXLDatum *pdxldatum) const = 0;

			// is type fixed length
			virtual
			BOOL FFixedLength() const = 0;

			// is type composite
			virtual
			BOOL FComposite() const = 0;

			// id of the relation corresponding to a composite type
			virtual
			IMDId *PmdidBaseRelation() const = 0;

			// type length
			virtual
			ULONG UlLength() const = 0;

			// is type passed by value
			virtual
			BOOL FByValue() const = 0;

			// return the null constant for this type
			virtual
			IDatum *PdatumNull() const = 0;

			// generate the DXL scalar constant from IDatum
			virtual
			CDXLScalarConstValue* PdxlopScConst(IMemoryPool *pmp, IDatum *pdatum) const = 0;

			// generate the DXL datum from IDatum
			virtual
			CDXLDatum* Pdxldatum(IMemoryPool *pmp, IDatum *pdatum) const = 0;

			// generate the DXL datum representing null value
			virtual
			CDXLDatum* PdxldatumNull(IMemoryPool *pmp) const = 0;
			
			// is type an ambiguous one? e.g., AnyElement in GPDB
			virtual
			BOOL FAmbiguous() const
			{
				return false;
			}

			// string representation of comparison types
			static
			const CWStringConst *PstrCmpType(IMDType::ECmpType ecmpt);

			// return true if we can perform statistical comparison between datums of these two types; else return false
			static
			BOOL FStatsComparable(const IMDType *pmdtypeFst, const IMDType *pmdtypeSnd);

			// return true if we can perform statistical comparison between datum of the given type and a given datum; else return false
			static
			BOOL FStatsComparable(const IMDType *pmdtypeFst, const IDatum *pdatumSnd);
	};
}

#endif // !GPMD_IMDCacheType_H

// EOF
