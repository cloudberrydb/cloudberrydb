//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2009 Greenplum, Inc.
//
//	@filename:
//		CDXLOperatorCost.h
//
//	@doc:
//		Representation of physical operator costs
//---------------------------------------------------------------------------
#ifndef GPDXL_CDXLOperatorCost_H
#define GPDXL_CDXLOperatorCost_H

#include "gpos/base.h"
#include "gpos/common/CRefCount.h"
#include "gpos/string/CWStringDynamic.h"

namespace gpdxl
{
	using namespace gpos;	

	// fwd decl
	class CXMLSerializer;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLOperatorCost
	//
	//	@doc:
	//		Class for representing costs of physical operators in the DXL tree
	//
	//---------------------------------------------------------------------------
	class CDXLOperatorCost : public CRefCount
	{
		private:
			
			// cost expended before fetching any tuples
			CWStringDynamic *m_pstrStartupCost;
			
			// total cost (assuming all tuples fetched)
			CWStringDynamic *m_pstrTotalCost;
			
			// number of rows plan is expected to emit
			CWStringDynamic *m_pstrRows;
			
			// average row width in bytes
			CWStringDynamic *m_pstrWidth;
			
			// private copy ctor
			CDXLOperatorCost(const CDXLOperatorCost &);
			
		public:
			// ctor/dtor
			CDXLOperatorCost
				(
				CWStringDynamic *pstrStartupCost,
				CWStringDynamic *pstrTotalCost,
				CWStringDynamic *pstrRows,
				CWStringDynamic *pstrWidth
				);
			
			virtual
			~CDXLOperatorCost();
			
			// serialize operator in DXL format
			void SerializeToDXL(CXMLSerializer *pxmlser) const;
			
			// accessors
			const CWStringDynamic *PstrStartupCost() const;
			const CWStringDynamic *PstrTotalCost() const;
			const CWStringDynamic *PstrRows() const;
			const CWStringDynamic *PstrWidth() const;
			
			// set the number of rows
			void SetRows(CWStringDynamic *pstr);
			
			// set the total cost
			void SetCost(CWStringDynamic *pstr);
	};
}


#endif // !GPDXL_CDXLOperatorCost_H

// EOF
