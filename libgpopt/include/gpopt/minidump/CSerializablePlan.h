//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CSerializablePlan.h
//
//	@doc:
//		Serializable plan object used to dump the query plan
//---------------------------------------------------------------------------
#ifndef GPOPT_CSerializablePlan_H
#define GPOPT_CSerializablePlan_H

#include "gpos/base.h"
#include "gpos/error/CSerializable.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

namespace gpopt
{
	//---------------------------------------------------------------------------
	//	@class:
	//		CSerializablePlan
	//
	//	@doc:
	//		Serializable plan object 
	//
	//---------------------------------------------------------------------------
	class CSerializablePlan : public CSerializable
	{
		private:

			IMemoryPool *m_pmp;

			// plan DXL node
			const CDXLNode *m_pdxlnPlan;

			// serialized plan
			CWStringDynamic *m_pstrPlan;
			
			// plan Id
			ULLONG m_ullPlanId;

			// plan space size
			ULLONG m_ullPlanSpaceSize;


			// private copy ctor
			CSerializablePlan(const CSerializablePlan&);

		public:

			// ctor
			CSerializablePlan(IMemoryPool *pmp, const CDXLNode *pdxlnPlan, ULLONG ullPlanId, ULLONG ullPlanSpaceSize);

			// dtor
			virtual
			~CSerializablePlan();

			// serialize object to passed buffer
			virtual
			void Serialize(COstream& oos);

	}; // class CSerializablePlan
}

#endif // !GPOPT_CSerializablePlan_H

// EOF

