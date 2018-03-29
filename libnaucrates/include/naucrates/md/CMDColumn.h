//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CMDColumn.h
//
//	@doc:
//		Class for representing metadata about relation's columns.
//---------------------------------------------------------------------------



#ifndef GPMD_CDXLColumn_H
#define GPMD_CDXLColumn_H

#include "gpos/base.h"

#include "naucrates/md/CMDName.h"
#include "naucrates/md/IMDColumn.h"


// fwd decl
namespace gpdxl
{
	class CDXLNode;
	class CXMLSerializer;
}

namespace gpmd
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDColumn
	//
	//	@doc:
	//		Class for representing metadata about relation's columns.
	//
	//---------------------------------------------------------------------------
	class CMDColumn : public IMDColumn
	{
		private:
			// attribute name
			CMDName *m_pmdname;
			
			// attribute number
			INT m_iAttNo;
			
			// column type
			IMDId *m_pmdidType;

			INT m_iTypeModifier;

			// is NULL an allowed value for the attribute
			BOOL m_fNullable;

			// is column dropped
			BOOL m_fDropped;
			
			// length of the column
			ULONG m_ulLength;
			
			// default value expression
			gpdxl::CDXLNode *m_pdxlnDefaultValue;
						
			// private copy ctor
			CMDColumn(const CMDColumn &);
		
		public:
			// ctor
			CMDColumn
				(
				CMDName *pmdname,
				INT iAttNo,
				IMDId *pmdidType,
				INT iTypeModifier,
				BOOL fNullable,
				BOOL fDropped,
				gpdxl::CDXLNode *pdxnlDefaultValue,
				ULONG ulLength = gpos::ulong_max
				);

			// dtor
			virtual
			~CMDColumn();

			// accessors
			virtual
			CMDName Mdname() const;
			
			// column type
			virtual 
			IMDId *PmdidType() const;

			virtual
			INT ITypeModifier() const;

			// attribute number
			virtual
			INT IAttno() const;
			
			// is this a system column
			virtual
			BOOL FSystemColumn() const
			{
				return (0 > m_iAttNo);
			}

			// length of the column
			ULONG UlLength() const
			{
				return m_ulLength;
			}

			// is the column nullable
			virtual
			BOOL FNullable() const;
			
			// is the column dropped
			virtual
			BOOL FDropped() const;
		
			// serialize metadata object in DXL format given a serializer object
			virtual	
			void Serialize(gpdxl::CXMLSerializer *) const;
			
#ifdef GPOS_DEBUG
			// debug print of the column
			virtual
			void DebugPrint(IOstream &os) const;
#endif
	};

	// array of metadata column descriptor
	typedef CDynamicPtrArray<CMDColumn, CleanupRelease> DrgPmdcol;

}

#endif // !GPMD_CDXLColumn_H

// EOF
