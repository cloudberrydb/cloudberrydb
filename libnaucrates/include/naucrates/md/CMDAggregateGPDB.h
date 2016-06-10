//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDAggregateGPDB.h
//
//	@doc:
//		Class for representing for GPDB-specific aggregates in the metadata cache
//---------------------------------------------------------------------------



#ifndef GPMD_CMDAggregateGPDB_H
#define GPMD_CMDAggregateGPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDAggregate.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"


namespace gpmd
{
	using namespace gpos;

	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDAggregateGPDB
	//
	//	@doc:
	//		Class for representing GPDB-specific aggregates in the metadata
	//		cache
	//
	//---------------------------------------------------------------------------
	class CMDAggregateGPDB : public IMDAggregate
	{		
		// memory pool
		IMemoryPool *m_pmp;
		
		// DXL for object
		const CWStringDynamic *m_pstr;
		
		// aggregate id
		IMDId *m_pmdid;
		
		// aggregate name
		CMDName *m_pmdname;
		
		// result type
		IMDId *m_pmdidTypeResult;
		
		// type of intermediate results
		IMDId *m_pmdidTypeIntermediate;

		// is aggregate ordered
		BOOL m_fOrdered;

		// is aggregate splittable
		BOOL m_fSplittable;
		
		// is aggregate hash capable
		BOOL m_fHashAggCapable;

		// private copy ctor
		CMDAggregateGPDB(const CMDAggregateGPDB &);
		
	public:
		// ctor
		CMDAggregateGPDB
			(
			IMemoryPool *pmp,
			IMDId *pmdid,
			CMDName *pmdname,
			IMDId *pmdidTypeResult,
			IMDId *pmdidTypeIntermediate,
			BOOL fOrderedAgg,
			BOOL fSplittable,
			BOOL fHashAggCapable
			);
		
		//dtor
		~CMDAggregateGPDB();
		
		// string representation of object
		virtual const CWStringDynamic *Pstr() const
		{
			return m_pstr;
		}
		
		// aggregate id
		virtual 
		IMDId *Pmdid() const;
		
		// aggregate name
		virtual 
		CMDName Mdname() const;
		
		// result id
		virtual 
		IMDId *PmdidTypeResult() const;
		
		// intermediate result id
		virtual 
		IMDId *PmdidTypeIntermediate() const;
		
		// serialize object in DXL format
		virtual 
		void Serialize(gpdxl::CXMLSerializer *pxmlser) const;
		
		// is an ordered aggregate
		virtual
		BOOL FOrdered() const
		{
			return m_fOrdered;
		}
		
		// is aggregate splittable
		virtual
		BOOL FSplittable() const
		{
			return m_fSplittable;
		}

		// is aggregate hash capable
		virtual
		BOOL FHashAggCapable() const
		{
			return m_fHashAggCapable;
		}

#ifdef GPOS_DEBUG
		// debug print of the type in the provided stream
		virtual
		void DebugPrint(IOstream &os) const;
#endif	
	};
}

#endif // !GPMD_CMDAggregateGPDB_H

// EOF
