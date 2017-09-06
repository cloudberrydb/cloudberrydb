//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2017 Pivotal Software Inc.
//
//	@filename:
//		CMDIndexInfo.h
//
//	@doc:
//		Implementation of indexinfo in relation metadata
//---------------------------------------------------------------------------

#ifndef GPMD_CMDIndexInfo_H
#define GPMD_CMDIndexInfo_H

#include "gpos/base.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/IMDInterface.h"

namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	// class for indexinfo in relation metadata
	class CMDIndexInfo : public IMDInterface
	{
		private:

			// index mdid
			IMDId *m_pmdid;

			// is the index partial
			BOOL m_fPartial;

		public:

			// ctor
			CMDIndexInfo
				(
				IMDId *pmdid,
				BOOL fIsPartial
				);

			// dtor
			virtual
			~CMDIndexInfo();

			// index mdid
			IMDId *Pmdid() const;

			// is the index partial
			BOOL FPartial() const;

			// serialize indexinfo in DXL format given a serializer object
			virtual
			void Serialize(CXMLSerializer *) const;

#ifdef GPOS_DEBUG
			// debug print of the index info
			virtual
			void DebugPrint(IOstream &os) const;
#endif

	};

}

#endif // !GPMD_CMDIndexInfo_H

// EOF
