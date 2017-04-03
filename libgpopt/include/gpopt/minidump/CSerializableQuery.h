//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CSerializableQuery.h
//
//	@doc:
//		Serializable query object used to dump the query when an exception is raised;
//---------------------------------------------------------------------------
#ifndef GPOS_CSerializableQuery_H
#define GPOS_CSerializableQuery_H

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
	//		CSerializableQuery
	//
	//	@doc:
	//		Serializable query object 
	//
	//---------------------------------------------------------------------------
	class CSerializableQuery : public CSerializable
	{
		private:

			// query DXL node;
			const CDXLNode *m_pdxlnQuery;

			// query output
			const DrgPdxln *m_pdrgpdxlnQueryOutput;

			// CTE DXL nodes
			const DrgPdxln *m_pdrgpdxlnCTE;

			IMemoryPool *m_pmp;
			
			// private copy ctor
			CSerializableQuery(const CSerializableQuery&);

		public:

			// ctor
			CSerializableQuery(const CDXLNode *pdxlnQuery, const DrgPdxln *pdrgpdxlnQueryOutput, const DrgPdxln *pdrgpdxlnCTE, IMemoryPool *pmp);

			// dtor
			virtual
			~CSerializableQuery();

			// serialize object to passed stream
			virtual
			void Serialize(COstream &oos);

	}; // class CSerializableQuery
}

#endif // !GPOS_CSerializableQuery_H

// EOF

