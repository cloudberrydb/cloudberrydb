//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CGPDBTypeHelper.h
//
//	@doc:
//		Helper class that provides implementation for common functions across
//		different GPDB types (CMDTypeInt4GPDB, CMDTypeBoolGPDB, and CMDTypeGenericGPDB)
//---------------------------------------------------------------------------
#ifndef GPMD_CGPDBHelper_H
#define GPMD_CGPDBHelper_H

#include "gpos/base.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"


namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	template<class T>
	class CGPDBTypeHelper
	{

		public:

			// serialize object in DXL format
			static void Serialize(CXMLSerializer *pxmlser, const T *pt);

#ifdef GPOS_DEBUG
			// debug print of the type in the provided stream
			static void DebugPrint(IOstream &os, const T *pt);
#endif // GPOS_DEBUG
	};
}

// include inline definition
#include "naucrates/md/CGPDBTypeHelper.inl"

#endif // !CGPMD_GPDBHelper_H

// EOF

