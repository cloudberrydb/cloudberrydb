//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLDatum.h
//
//	@doc:
//		Class for representing DXL datums
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLDatum_H
#define GPDXL_CDXLDatum_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/string/CWStringConst.h"

#include "naucrates/md/IMDId.h"
namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	// fwd decl
	class CXMLSerializer;

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLDatum
	//
	//	@doc:
	//		Class for representing DXL datums
	//
	//---------------------------------------------------------------------------
	class CDXLDatum : public CRefCount
	{
		private:

			// private copy ctor
			CDXLDatum(const CDXLDatum &);

		protected:

			// memory pool
			IMemoryPool *m_pmp;
			
			// mdid of the datum's type
			IMDId *m_pmdidType;

			const INT m_iTypeModifier;

			// is the datum NULL
			BOOL m_fNull;
	
			// length
			const ULONG m_ulLength;
			
		public:

			// datum types
			enum EdxldatumType
			{
				EdxldatumInt2,
				EdxldatumInt4,
				EdxldatumInt8,
				EdxldatumBool,
				EdxldatumGeneric,
				EdxldatumStatsDoubleMappable,
				EdxldatumStatsLintMappable,
				EdxldatumOid,
				EdxldatumSentinel
			};
			// ctor
			CDXLDatum
				(
				IMemoryPool *pmp,
				IMDId *pmdidType,
				INT iTypeModifier,
				BOOL fNull,
				ULONG ulLength
				);

			// dtor
			virtual
			~CDXLDatum()
			{
				m_pmdidType->Release();
			}

			// mdid type of the datum
			virtual
			IMDId *Pmdid() const
			{
				return m_pmdidType;
			}

			INT
			ITypeModifier() const;

			// is datum NULL
			virtual
			BOOL FNull() const;

			// byte array length
			virtual
			ULONG UlLength() const;

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser, const CWStringConst *pstrElem);

			// is type passed by value
			virtual
			BOOL FByValue() const = 0;

			// serialize the datum as the given element
			virtual
			void Serialize(CXMLSerializer *pxmlser) = 0;

			// ident accessors
			virtual
			EdxldatumType Edxldt() const = 0;
	};

	// array of datums
	typedef CDynamicPtrArray<CDXLDatum, CleanupRelease> DrgPdxldatum;

	// dynamic array of datum arrays -- array owns elements
	typedef CDynamicPtrArray<DrgPdxldatum, CleanupRelease> DrgPdrgPdxldatum;
}

#endif // !GPDXL_CDXLDatum_H

// EOF

