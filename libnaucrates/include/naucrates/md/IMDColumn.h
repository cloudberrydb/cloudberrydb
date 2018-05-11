//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		IMDColumn.h
//
//	@doc:
//		Interface class for columns in a metadata cache relation
//---------------------------------------------------------------------------



#ifndef GPMD_IMDColumn_H
#define GPMD_IMDColumn_H

#include "gpos/base.h"

#include "naucrates/dxl/gpdb_types.h"

#include "naucrates/md/IMDInterface.h"
#include "naucrates/md/IMDId.h"

namespace gpmd
{
	using namespace gpos;

	class CMDName;

	//---------------------------------------------------------------------------
	//	@class:
	//		IMDColumn
	//
	//	@doc:
	//		Interface class for columns in a metadata cache relation
	//
	//---------------------------------------------------------------------------
	class IMDColumn : public IMDInterface
	{		
		public:
		
			// column name
			virtual 
			CMDName Mdname() const = 0;
			
			// id of attribute type
			virtual 
			IMDId *PmdidType() const = 0;

			virtual
			INT ITypeModifier() const = 0;

			// are nulls allowed for this column
			virtual 
			BOOL FNullable() const = 0;
			
			// attribute number in the system catalog
			virtual 
			INT IAttno() const = 0;
			
			// is this a system column
			virtual 
			BOOL FSystemColumn() const = 0;
			
			// is column dropped
			virtual
			BOOL FDropped() const = 0;

			// length of the column
			virtual ULONG UlLength() const = 0;

#ifdef GPOS_DEBUG
			// debug print of the column
			virtual 
			void DebugPrint(IOstream &os) const = 0;
#endif
	};

	// IMDColumn array
//	typedef CDynamicPtrArray<IMDColumn, CleanupRelease> DrgPmdcol;

}



#endif // !GPMD_IMDColumn_H

// EOF
