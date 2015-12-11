//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLMinidump.h
//
//	@doc:
//		DXL-based minidump structure
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CDXLMinidump_H
#define GPOPT_CDXLMinidump_H

#include "gpos/base.h"

#include "naucrates/dxl/CDXLUtils.h"

// fwd decl
namespace gpos
{
	class CBitSet;
}

namespace gpdxl
{
	class CDXLNode;
}

using namespace gpos;
using namespace gpdxl;

namespace gpopt
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLMinidump
	//
	//	@doc:
	//		DXL-based minidump
	//
	//---------------------------------------------------------------------------
	class CDXLMinidump
	{
		private:
			// traceflags
			CBitSet *m_pbs;
			
			// optimizer configuration
			COptimizerConfig *m_poconf;
			
			// DXL query tree
			CDXLNode *m_pdxlnQuery;
			
			// Array of DXL nodes that represent the query output
			DrgPdxln *m_pdrgpdxlnQueryOutput;
			
			// Array of DXL nodes that represent the CTE producers
			DrgPdxln *m_pdrgpdxlnCTE;

			// DXL plan
			CDXLNode *m_pdxlnPlan;

			// metadata objects
			DrgPimdobj *m_pdrgpmdobj;
			
			// source system ids
			DrgPsysid *m_pdrgpsysid;
			
			// plan Id
			ULLONG m_ullPlanId;

			// plan space size
			ULLONG m_ullPlanSpaceSize;

			// private copy ctor
			CDXLMinidump(const CDXLMinidump&);

		public:

			// ctor
			CDXLMinidump
				(
				CBitSet *pbs, 
				COptimizerConfig *poconf,
				CDXLNode *pdxlnQuery, 
				DrgPdxln *pdrgpdxlnQueryOutput,
				DrgPdxln *pdrgpdxlnCTE,
				CDXLNode *pdxlnPlan, 
				DrgPimdobj *pdrgpmdobj, 
				DrgPsysid *pdrgpsysid,
				ULLONG ullPlanId,
				ULLONG ullPlanSpaceSize
				);

			// dtor
			~CDXLMinidump();
			
			// traceflags
			const CBitSet *Pbs() const;
			
			// optimizer configuration
			COptimizerConfig *Poconf() const
			{
				return m_poconf;
			}

			// query object
			const CDXLNode *PdxlnQuery() const;
			
			// query output columns
			const DrgPdxln *PdrgpdxlnQueryOutput() const;
			
			// CTE list
			const DrgPdxln *PdrgpdxlnCTE() const;

			// plan
			const CDXLNode *PdxlnPlan() const;

			// metadata objects
			const DrgPimdobj *Pdrgpmdobj() const;
			
			// source system ids
			const DrgPsysid *Pdrgpsysid() const;
			
			// return plan id
			ULLONG UllPlanId() const;

			// return plan space size
			ULLONG UllPlanSpaceSize() const;

	}; // class CDXLMinidump
}

#endif // !GPOPT_CDXLMinidump_H

// EOF

