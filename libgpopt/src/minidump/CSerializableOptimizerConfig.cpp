//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2018 Pivotal, Inc..
//
//	@filename:
//		CSerializableOptimizerConfig.cpp
//
//	@doc:
//		Class for serializing optimizer config objects
//---------------------------------------------------------------------------

#include "gpos/base.h"
#include "gpos/error/CErrorContext.h"
#include "gpos/task/CTask.h"

#include "naucrates/dxl/xml/CDXLSections.h"
#include "naucrates/dxl/CDXLUtils.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/minidump/CSerializableOptimizerConfig.h"
#include "gpopt/base/COptCtxt.h"

using namespace gpos;
using namespace gpopt;
using namespace gpdxl;

// max length of the string representation of traceflag codes
#define GPOPT_MAX_TRACEFLAG_CODE_LENGTH 10

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::CSerializableOptimizerConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CSerializableOptimizerConfig::CSerializableOptimizerConfig
	(
	IMemoryPool *pmp,
	const COptimizerConfig *poconf
	)
	:
	CSerializable(),
	m_pmp(pmp),
	m_poconf(poconf)
{
	GPOS_ASSERT(NULL != poconf);
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::~CSerializableOptimizerConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CSerializableOptimizerConfig::~CSerializableOptimizerConfig()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CSerializableOptimizerConfig::Serialize
//
//	@doc:
//		Serialize contents into provided stream
//
//---------------------------------------------------------------------------
void
CSerializableOptimizerConfig::Serialize
	(
	COstream &oos
	)
{
	CXMLSerializer xmlser(m_pmp, oos, false /*Indent*/);

	// Copy traceflags from global state
	CBitSet *pbs = CTask::PtskSelf()->Ptskctxt()->PbsCopyTraceFlags(m_pmp);
	m_poconf->Serialize(m_pmp, &xmlser, pbs);
	pbs->Release();
}

// EOF

