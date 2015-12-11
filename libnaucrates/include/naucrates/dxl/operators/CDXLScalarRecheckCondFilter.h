//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal, Inc.
//
//	@filename:
//		CDXLScalarRecheckCondFilter.h
//
//	@doc:
//		Filter for rechecking an index condition on the operator upstream of the index scan
//
//	@owner:
//		
//
//	@test:
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLScalarRecheckCondFilter_H
#define GPDXL_CDXLScalarRecheckCondFilter_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLScalarFilter.h"

namespace gpdxl
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLScalarRecheckCondFilter
	//
	//	@doc:
	//		Filter for rechecking an index condition on the operator upstream of the bitmap index scan
	//
	//---------------------------------------------------------------------------
	class CDXLScalarRecheckCondFilter : public CDXLScalarFilter
	{
		private:
			// private copy ctor
			CDXLScalarRecheckCondFilter(CDXLScalarRecheckCondFilter &);

		public:
			// ctor
			explicit
			CDXLScalarRecheckCondFilter
				(
				IMemoryPool *pmp
				)
				:
				CDXLScalarFilter(pmp)
			{
			}

			// operator identity
			virtual
			Edxlopid Edxlop() const
			{
				return EdxlopScalarRecheckCondFilter;
			}

			// operator name
			virtual
			const CWStringConst *PstrOpName() const;

			// does the operator return a boolean result
			virtual
			BOOL FBoolean
					(
					CMDAccessor *//pmda
					)
					const
			{
				GPOS_ASSERT(!"Invalid function call for a container operator");
				return false;
			}

			// conversion function
			static
			CDXLScalarRecheckCondFilter *PdxlopConvert
				(
				CDXLOperator *pdxlop
				)
			{
				GPOS_ASSERT(NULL != pdxlop);
				GPOS_ASSERT(EdxlopScalarRecheckCondFilter == pdxlop->Edxlop());

				return dynamic_cast<CDXLScalarRecheckCondFilter*>(pdxlop);
			}
	};
}

#endif // !GPDXL_CDXLScalarRecheckCondFilter_H

// EOF
