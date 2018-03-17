//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc.
//
//	@filename:
//		COptimizerConfig.h
//
//	@doc:
//		Configurations used by the optimizer
//---------------------------------------------------------------------------

#ifndef GPOPT_COptimizerConfig_H
#define GPOPT_COptimizerConfig_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"

#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CCTEConfig.h"
#include "gpopt/engine/CHint.h"
#include "gpopt/base/CWindowOids.h"

namespace gpopt
{
	using namespace gpos;

	// forward decl
	class ICostModel;

	//---------------------------------------------------------------------------
	//	@class:
	//		COptimizerConfig
	//
	//	@doc:
	//		Configurations used by the optimizer
	//
	//---------------------------------------------------------------------------
	class COptimizerConfig : public CRefCount
	{

		private:
			
			// plan enumeration configuration
			CEnumeratorConfig *m_pec;

			// statistics configuration
			CStatisticsConfig *m_pstatsconf;

			// CTE configuration
			CCTEConfig *m_pcteconf;
			
			// cost model configuration
			ICostModel *m_pcm;

			// hint configuration
			CHint *m_phint;

			// default window oids
			CWindowOids *m_pwindowoids;

		public:

			// ctor
			COptimizerConfig
				(
				CEnumeratorConfig *pec,
				CStatisticsConfig *pstatsconf,
				CCTEConfig *pcteconf,
				ICostModel *pcm,
				CHint *phint,
				CWindowOids *pdefoidsGPDB
				);

			// dtor
			virtual
			~COptimizerConfig();

			
			// plan enumeration configuration
			CEnumeratorConfig *Pec() const
			{
				return m_pec;
			}

			// statistics configuration
			CStatisticsConfig *Pstatsconf() const
			{
				return m_pstatsconf;
			}

			// CTE configuration
			CCTEConfig *Pcteconf() const
			{
				return m_pcteconf;
			}

			// cost model configuration
			ICostModel *Pcm() const
			{
				return m_pcm;
			}
			
			// default window oids
			CWindowOids *Pwindowoids() const
			{
				return m_pwindowoids;
			}

			// hint configuration
			CHint *Phint() const
			{
				return m_phint;
			}

			// generate default optimizer configurations
			static
			COptimizerConfig *PoconfDefault(IMemoryPool *pmp);
			
			// generate default optimizer configurations with the given cost model
			static
			COptimizerConfig *PoconfDefault(IMemoryPool *pmp, ICostModel *pcm);

			void Serialize(IMemoryPool *pmp, CXMLSerializer *pxmlser, CBitSet *pbsTrace) const;

	}; // class COptimizerConfig

}

#endif // !GPOPT_COptimizerConfig_H

// EOF
