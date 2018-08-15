//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Greenplum
//
//	@filename:
//		CGPDBAttOptCol.h
//
//	@doc:
//		Class to represent pair of GPDB var info to optimizer col info
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CGPDBAttOptCol_H
#define GPDXL_CGPDBAttOptCol_H

#include "gpos/common/CRefCount.h"
#include "gpopt/translate/CGPDBAttInfo.h"
#include "gpopt/translate/COptColInfo.h"

namespace gpdxl
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CGPDBAttOptCol
	//
	//	@doc:
	//		Class to represent pair of GPDB var info to optimizer col info
	//
	//---------------------------------------------------------------------------
	class CGPDBAttOptCol: public CRefCount
	{
		private:

			// gpdb att info
			CGPDBAttInfo *m_pgpdbattinfo;

			// optimizer col info
			COptColInfo *m_poptcolinfo;

			// copy c'tor
			CGPDBAttOptCol(const CGPDBAttOptCol&);

		public:
			// ctor
			CGPDBAttOptCol(CGPDBAttInfo *pgpdbattinfo, COptColInfo *poptcolinfo)
				: m_pgpdbattinfo(pgpdbattinfo), m_poptcolinfo(poptcolinfo)
			{
				GPOS_ASSERT(NULL != m_pgpdbattinfo);
				GPOS_ASSERT(NULL != m_poptcolinfo);
			}

			// d'tor
			virtual
			~CGPDBAttOptCol()
			{
				m_pgpdbattinfo->Release();
				m_poptcolinfo->Release();
			}

			// accessor
			const CGPDBAttInfo *Pgpdbattinfo() const
			{
				return m_pgpdbattinfo;
			}

			// accessor
			const COptColInfo *Poptcolinfo() const
			{
				return m_poptcolinfo;
			}

	};

}

#endif // !GPDXL_CGPDBAttOptCol_H

// EOF
