//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CSerializableOptimizerConfig.h
//
//	@doc:
//		Serializable optimizer configuration object used to use for minidumping
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOS_CSerializableOptimizerConfig_H
#define GPOS_CSerializableOptimizerConfig_H

#include "gpos/base.h"
#include "gpos/error/CSerializable.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLNode.h"

using namespace gpos;
using namespace gpdxl;

namespace gpopt
{

	// fwd decl
	class COptimizerConfig;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CSerializableOptimizerConfig
	//
	//	@doc:
	//		Serializable optimizer configuration object
	//
	//---------------------------------------------------------------------------
	class CSerializableOptimizerConfig : public CSerializable
	{
		private:

			// optimizer configurations
			const COptimizerConfig *m_poconf;

			// serialized optimizer configuration options
			CWStringDynamic *m_pstrOptimizerConfig;
			
			// private copy ctor
			CSerializableOptimizerConfig(const CSerializableOptimizerConfig&);

			// calculate space needed for serialization of traceflags
			ULONG_PTR UlpRequiredSpaceTraceflags();

			// serialize traceflags to passed buffer
			ULONG_PTR UlpSerializeTraceflags(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize);

			// serialize footer
			ULONG_PTR UlpSerializeFooter(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize);
			
		public:

			// ctor
			CSerializableOptimizerConfig(const COptimizerConfig *poconf);

			// dtor
			virtual
			~CSerializableOptimizerConfig();

			// serialize optimizer configurations to string
			void Serialize(IMemoryPool *pmp);

			// calculate space needed for serialization
			virtual
			ULONG_PTR UlpRequiredSpace();

			// serialize object to passed buffer
			virtual
			ULONG_PTR UlpSerialize(WCHAR *wszBuffer, ULONG_PTR ulpAllocSize);

	}; // class CSerializableOptimizerConfig
}

#endif // !GPOS_CSerializableOptimizerConfig_H

// EOF

