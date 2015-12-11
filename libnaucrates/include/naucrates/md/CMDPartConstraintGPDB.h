//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDPartConstraintGPDB.h
//
//	@doc:
//		Class representing a GPDB partition constraint in the MD cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPMD_CMDPartConstraintGPDB_H
#define GPMD_CMDPartConstraintGPDB_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDPartConstraint.h"
#include "naucrates/md/CMDName.h"

// fwd decl
namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpopt
{
	class CExpression;
	class CMDAccessor;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDPartConstraintGPDB
	//
	//	@doc:
	//		Class representing a GPDB partition constraint in the MD cache
	//
	//---------------------------------------------------------------------------
	class CMDPartConstraintGPDB : public IMDPartConstraint
	{
		private:

			// memory pool
			IMemoryPool *m_pmp;

			// the DXL representation of the part constraint
			CDXLNode *m_pdxln;

			// included default partitions
			DrgPul *m_pdrgpulDefaultParts;
			
			// is constraint unbounded
			BOOL m_fUnbounded;
			
		public:

			// ctor
			CMDPartConstraintGPDB(IMemoryPool *pmp, CDXLNode *pdxln, DrgPul *pdrgpulDefaultParts, BOOL fUnbounded);

			// dtor
			virtual
			~CMDPartConstraintGPDB();

			// serialize constraint in DXL format
			virtual
			void Serialize(CXMLSerializer *pxmlser) const;
			
			// the scalar expression of the check constraint
			virtual
			CExpression *Pexpr(IMemoryPool *pmp, CMDAccessor *pmda, DrgPcr *pdrgpcr) const;
			
			// included default partitions
			virtual
			DrgPul *PdrgpulDefaultParts() const;

			// is constraint unbounded
			virtual
			BOOL FUnbounded() const;

	};
}

#endif // !GPMD_CMDPartConstraintGPDB_H

// EOF

