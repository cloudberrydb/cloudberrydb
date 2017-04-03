//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CSerializableOptimizerConfig.h
//
//	@doc:
//		Serializable optimizer configuration object used to use for minidumping
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

			IMemoryPool *m_pmp;

			// private copy ctor
			CSerializableOptimizerConfig(const CSerializableOptimizerConfig&);

			// serialize traceflags to passed stream
			void SerializeTraceflags(COstream& oos);
			
		public:

			// ctor
			CSerializableOptimizerConfig(const COptimizerConfig *poconf, IMemoryPool *pmp);

			// dtor
			virtual
			~CSerializableOptimizerConfig();

			// serialize object to passed stream
			virtual
			void Serialize(COstream& oos);

	}; // class CSerializableOptimizerConfig
}

#endif // !GPOS_CSerializableOptimizerConfig_H

// EOF

