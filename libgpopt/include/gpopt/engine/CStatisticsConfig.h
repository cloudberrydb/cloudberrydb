//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CStatisticsConfig.h
//
//	@doc:
//		Statistics configurations
//---------------------------------------------------------------------------
#ifndef GPOPT_CStatisticsConfig_H
#define GPOPT_CStatisticsConfig_H

#include "gpos/base.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/common/CRefCount.h"
#include "gpos/common/CDouble.h"
#include "gpos/sync/CAutoMutex.h"

#include "naucrates/md/IMDId.h"
#include "naucrates/md/CMDIdColStats.h"

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;

	//---------------------------------------------------------------------------
	//	@class:
	//		CStatisticsConfig
	//
	//	@doc:
	//		Statistics configurations
	//
	//---------------------------------------------------------------------------
	class CStatisticsConfig : public CRefCount
	{

		private:

			// shared memory pool
			IMemoryPool *m_pmp;

			// damping factor for filter
			CDouble m_dDampingFactorFilter;

			// damping factor for join
			CDouble m_dDampingFactorJoin;

			// damping factor for group by
			CDouble m_dDampingFactorGroupBy;

			// hash set of md ids for columns with missing statistics
			HSMDId *m_phsmdidcolinfo;

			// mutex for locking entry when accessing / changing missing statistics column info
			CMutex m_mutexMissingColStats;

		public:

			// ctor
			CStatisticsConfig
				(
				IMemoryPool *pmp,
				CDouble dDampingFactorFilter,
				CDouble dDampingFactorJoin,
				CDouble dDampingFactorGroupBy
				);

			// dtor
			~CStatisticsConfig();

			// damping factor for filter
			CDouble DDampingFactorFilter() const
			{
				return m_dDampingFactorFilter;
			}

			// damping factor for join
			CDouble DDampingFactorJoin() const
			{
				return m_dDampingFactorJoin;
			}

			// damping factor for group by
			CDouble DDampingFactorGroupBy() const
			{
				return m_dDampingFactorGroupBy;
			}

			// add the information about the column with the missing statistics
			void AddMissingStatsColumn(CMDIdColStats *pmdidCol);

			// collect the missing statistics columns
			void CollectMissingStatsColumns(DrgPmdid *pdrgmdid);

			// generate default optimizer configurations
			static
			CStatisticsConfig *PstatsconfDefault(IMemoryPool *pmp)
			{
				return GPOS_NEW(pmp) CStatisticsConfig
									(
									pmp,
									0.75 /* dDampingFactorFilter */,
									0.01 /* dDampingFactorJoin */,
									0.75 /* dDampingFactorGroupBy */
									);
			}


	}; // class CStatisticsConfig
}

#endif // !GPOPT_CStatisticsConfig_H

// EOF
