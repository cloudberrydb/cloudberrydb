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
			CSerializablePlan(const CDXLNode *pdxlnPlan, ULLONG ullPlanId, ULLONG ullPlanSpaceSize);

			// dtor
			virtual
			~CSerializablePlan();

			// calculate space needed for serialization
			virtual
			ULONG_PTR UlpRequiredSpace()
			{
				if (NULL == m_pstrPlan)
				{
					return 0;
				}
				
				return m_pstrPlan->UlLength();
			}

			// serialize plan to string
			void Serialize(IMemoryPool *pmp);

			// serialize object to passed buffer
			virtual
			ULONG_PTR UlpSerialize(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize);

	}; // class CSerializablePlan
}

#endif // !GPOPT_CSerializablePlan_H

// EOF

