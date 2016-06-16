//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMDCheckConstraintGPDB.h
//
//	@doc:
//		Class representing a GPDB check constraint in a metadata cache relation
//---------------------------------------------------------------------------

#ifndef GPMD_CMDCheckConstraintGPDB_H
#define GPMD_CMDCheckConstraintGPDB_H

#include "gpos/base.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDCheckConstraint.h"
#include "naucrates/md/CMDName.h"

// fwd decl
namespace gpdxl
{
	class CXMLSerializer;
}

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDCheckConstraintGPDB
	//
	//	@doc:
	//		Class representing a GPDB check constraint in a metadata cache relation
	//
	//---------------------------------------------------------------------------
	class CMDCheckConstraintGPDB : public IMDCheckConstraint
	{
		private:

			// memory pool
			IMemoryPool *m_pmp;

			// check constraint mdid
			IMDId *m_pmdid;

			// check constraint name
			CMDName *m_pmdname;

			// relation mdid
			IMDId *m_pmdidRel;

			// the DXL representation of the check constraint
			CDXLNode *m_pdxln;

			// DXL for object
			const CWStringDynamic *m_pstr;

		public:

			// ctor
			CMDCheckConstraintGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdname,
				IMDId *pmdidRel,
				CDXLNode *pdxln
				);

			// dtor
			virtual
			~CMDCheckConstraintGPDB();

			// check constraint mdid
			virtual
			IMDId *Pmdid() const
			{
				return m_pmdid;
			}

			// check constraint name
			virtual
			CMDName Mdname() const
			{
				return *m_pmdname;
			}

			// mdid of the relation
			virtual
			IMDId *PmdidRel() const
			{
				return m_pmdidRel;
			}

			// DXL string for check constraint
			virtual
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}

			// the scalar expression of the check constraint
			virtual
			CExpression *Pexpr(IMemoryPool *pmp, CMDAccessor *pmda, DrgPcr *pdrgpcr) const;

			// serialize MD check constraint in DXL format given a serializer object
			virtual
			void Serialize(gpdxl::CXMLSerializer *) const;

#ifdef GPOS_DEBUG
			// debug print of the MD check constraint
			virtual
			void DebugPrint(IOstream &os) const;
#endif
	};
}

#endif // !GPMD_CMDCheckConstraintGPDB_H

// EOF

