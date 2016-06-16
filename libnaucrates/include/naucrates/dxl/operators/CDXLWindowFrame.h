//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CDXLWindowFrame.h
//
//	@doc:
//		Class for representing DXL window frame
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLWindowFrame_H
#define GPDXL_CDXLWindowFrame_H

#include "gpos/base.h"
#include "naucrates/dxl/operators/CDXLScalar.h"
#include "naucrates/md/IMDId.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	enum EdxlFrameSpec
	{
		EdxlfsRow = 0,
		EdxlfsRange,
		EdxlfsSentinel
	};

	enum EdxlFrameExclusionStrategy
	{
		EdxlfesNone = 0,
		EdxlfesNulls,
		EdxlfesCurrentRow,
		EdxlfesGroup,
		EdxlfesTies,
		EdxlfesSentinel
	};

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLWindowFrame
	//
	//	@doc:
	//		Class for representing DXL window frame
	//
	//---------------------------------------------------------------------------
	class CDXLWindowFrame : public CRefCount
	{
		private:

			// memory pool;
			IMemoryPool *m_pmp;

			// row or range based window specification method
			EdxlFrameSpec m_edxlfs;

			// exclusion strategy
			EdxlFrameExclusionStrategy m_edxlfes;

			// private copy ctor
			CDXLWindowFrame(const CDXLWindowFrame&);

			// scalar value representing the boundary leading
			CDXLNode *m_pdxlnLeading;

			// scalar value representing the boundary trailing
			CDXLNode *m_pdxlnTrailing;

		public:
			// ctor
			CDXLWindowFrame
				(
				IMemoryPool *pmp,
				EdxlFrameSpec edxlfs,
				EdxlFrameExclusionStrategy edxlfes,
				CDXLNode *pdxlnLeading,
				CDXLNode *pdxlnTrailing
				);

			//dtor
			virtual
			~CDXLWindowFrame();

			EdxlFrameSpec Edxlfs() const
			{
				return  m_edxlfs;
			}

			// exclusion strategy
			EdxlFrameExclusionStrategy Edxlfes() const
			{
				return m_edxlfes;
			}

			// return window boundary trailing
			CDXLNode *PdxlnTrailing() const
			{
				return m_pdxlnTrailing;
			}

			// return window boundary leading
			CDXLNode *PdxlnLeading() const
			{
				return m_pdxlnLeading;
			}
			
			// return the string representation of the exclusion strategy
			const CWStringConst *PstrES(EdxlFrameExclusionStrategy edxles) const;
			
			// return the string representation of the frame specification (row or range)
			const CWStringConst *PstrFS(EdxlFrameSpec edxlfs) const;

			// serialize operator in DXL format
			virtual
			void SerializeToDXL(CXMLSerializer *pxmlser) const;
	};
}

#endif // !GPDXL_CDXLWindowFrame_H

// EOF
