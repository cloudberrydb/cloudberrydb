//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CColumnDescriptor.h
//
//	@doc:
//		Abstraction of columns in tables, functions, external tables etc.
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CColumnDescriptor_H
#define GPOPT_CColumnDescriptor_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/metadata/CName.h"

#include "naucrates/md/IMDType.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CColumnDescriptor
	//
	//	@doc:
	//		Metadata abstraction for columns that exist in the catalog; 
	//		Transient columns as computed during query execution do not have a 
	//		column descriptor;
	//
	//---------------------------------------------------------------------------
	class CColumnDescriptor : public CRefCount
	{
		private:

			// type information
			const IMDType *m_pmdtype;

			// name of column -- owned
			CName m_name;			
			
			// attribute number
			INT m_iAttno;
			
			// does column allow null values?
			BOOL m_fNullable;

			// width of the column, for instance  char(10) column has width 10
			ULONG m_ulWidth;

		public:

			// ctor
			CColumnDescriptor
				(
				IMemoryPool *pmp,
				const IMDType *pmdtype,
				const CName &name,
				INT iAttno,
				BOOL fNullable,
				ULONG ulWidth = ULONG_MAX
				);

			// dtor
			virtual
			~CColumnDescriptor();

			// return column name
			const CName &Name() const
			{
				return m_name;
			}
			
			// return metadata type
			const IMDType *Pmdtype() const
			{
				return m_pmdtype;
			}
			
			// return attribute number
			INT IAttno() const
			{
				return m_iAttno;
			}

			// does column allow null values?
			BOOL FNullable() const
			{
				return m_fNullable;
			}
	
			// is this a system column
			virtual
			BOOL FSystemColumn() const
			{
				return (0 > m_iAttno);
			}

			// width of the column
			virtual
			ULONG UlWidth() const
			{
				return m_ulWidth;
			}

#ifdef GPOS_DEBUG
			IOstream &OsPrint(IOstream &) const;
#endif // GPOS_DEBUG

	}; // class CColumnDescriptor
}

#endif // !GPOPT_CColumnDescriptor_H

// EOF
