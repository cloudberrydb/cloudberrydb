//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLRelStats.h
//
//	@doc:
//		Class representing relation stats
//---------------------------------------------------------------------------



#ifndef GPMD_CDXLRelStats_H
#define GPMD_CDXLRelStats_H

#include "gpos/base.h"
#include "gpos/common/CDouble.h"

#include "gpos/string/CWStringDynamic.h"

#include "naucrates/md/IMDRelStats.h"
#include "naucrates/md/CMDIdRelStats.h"

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
	//		CDXLRelStats
	//
	//	@doc:
	//		Class representing relation stats
	//
	//---------------------------------------------------------------------------
	class CDXLRelStats : public IMDRelStats
	{		
		private:
		
			// memory pool
			IMemoryPool *m_pmp;

			// metadata id of the object
			CMDIdRelStats *m_pmdidRelStats;
			
			// table name
			CMDName *m_pmdname;
			
			// number of rows
			CDouble m_dRows;
			
			// flag to indicate if input relation is empty
			BOOL m_fEmpty;

			// DXL string for object
			CWStringDynamic *m_pstr;
			
			// private copy ctor
			CDXLRelStats(const CDXLRelStats &);
		
		public:
			
			CDXLRelStats
				(
				IMemoryPool *pmp,
				CMDIdRelStats *pmdidRelStats,
				CMDName *pmdname,
				CDouble dRows,
				BOOL fEmpty
				);
			
			virtual
			~CDXLRelStats();
			
			// the metadata id
			virtual 
			IMDId *Pmdid() const;
			
			// relation name
			virtual 
			CMDName Mdname() const;
			
			// DXL string representation of cache object 
			virtual 
			const CWStringDynamic *Pstr() const;
			
			// number of rows
			virtual
			CDouble DRows() const;
			
			// is statistics on an empty input
			virtual
			BOOL FEmpty() const
			{
				return m_fEmpty;
			}

			// serialize relation stats in DXL format given a serializer object
			virtual 
			void Serialize(gpdxl::CXMLSerializer *) const;

#ifdef GPOS_DEBUG
			// debug print of the metadata relation
			virtual 
			void DebugPrint(IOstream &os) const;
#endif

			// dummy relstats
			static
			CDXLRelStats *PdxlrelstatsDummy
								(
								IMemoryPool *pmp,
								IMDId *pmdid
								);
	};

}



#endif // !GPMD_CDXLRelStats_H

// EOF
