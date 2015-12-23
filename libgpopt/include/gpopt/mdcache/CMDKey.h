//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDKey.h
//
//	@doc:
//		Key for metadata objects in the cache
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPOPT_CMDKey_H
#define GPOPT_CMDKey_H

#include "gpos/base.h"
#include "gpos/string/CWStringConst.h"

#include "naucrates/md/IMDId.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;
	

	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMDKey
	//
	//	@doc:
	//		Key for metadata objects in the cache
	//
	//---------------------------------------------------------------------------
	class CMDKey
	{
		private:
			// id of the object in the underlying source
			const IMDId *m_pmdid;
			
		public:
			// ctors
			explicit
			CMDKey(const IMDId *pmdid);
			
			// dtor
			~CMDKey()
			{}

			
			const IMDId *Pmdid() const
			{
				return m_pmdid;
			}
					
			// equality function
			BOOL FEquals(const CMDKey &mdkey) const;
			
			// hash function
			ULONG UlHash() const;
			
			
			// equality function for using MD keys in a cache
			static BOOL FEqualMDKey(CMDKey* const &pvLeft, CMDKey* const &pvRight);
			
			// hash function for using MD keys in a cache
			static ULONG UlHashMDKey(CMDKey* const & pv);
			
	};
}



#endif // !GPOPT_CMDKey_H

// EOF
