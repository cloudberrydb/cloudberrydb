//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerOptimizerConfig.cpp
//
//	@doc:
//		Implementation of the SAX parse handler class for parsing optimizer 
//		config params
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#include "gpos/common/CBitSet.h"

#include "naucrates/dxl/parser/CParseHandlerOptimizerConfig.h"
#include "naucrates/dxl/parser/CParseHandlerManager.h"
#include "naucrates/dxl/parser/CParseHandlerFactory.h"
#include "naucrates/dxl/parser/CParseHandlerTraceFlags.h"
#include "naucrates/dxl/parser/CParseHandlerEnumeratorConfig.h"
#include "naucrates/dxl/parser/CParseHandlerStatisticsConfig.h"
#include "naucrates/dxl/parser/CParseHandlerCTEConfig.h"
#include "naucrates/dxl/parser/CParseHandlerCostModel.h"
#include "naucrates/dxl/parser/CParseHandlerHint.h"

#include "naucrates/dxl/operators/CDXLOperatorFactory.h"
#include "naucrates/traceflags/traceflags.h"

#include "naucrates/dxl/xml/dxltokens.h"

#include "gpopt/engine/CEnumeratorConfig.h"
#include "gpopt/engine/CStatisticsConfig.h"
#include "gpopt/optimizer/COptimizerConfig.h"
#include "gpopt/cost/ICostModel.h"

using namespace gpdxl;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOptimizerConfig::CParseHandlerOptimizerConfig
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CParseHandlerOptimizerConfig::CParseHandlerOptimizerConfig
	(
	IMemoryPool *pmp,
	CParseHandlerManager *pphm,
	CParseHandlerBase *pphRoot
	)
	:
	CParseHandlerBase(pmp, pphm, pphRoot),
	m_pbs(NULL),
	m_poconf(NULL)
{
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOptimizerConfig::~CParseHandlerOptimizerConfig
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CParseHandlerOptimizerConfig::~CParseHandlerOptimizerConfig()
{
	CRefCount::SafeRelease(m_pbs);
	CRefCount::SafeRelease(m_poconf);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOptimizerConfig::StartElement
//
//	@doc:
//		Invoked by Xerces to process an opening tag
//
//---------------------------------------------------------------------------
void
CParseHandlerOptimizerConfig::StartElement
	(
	const XMLCh* const xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const xmlszQname,
	const Attributes &attrs
	)
{	
	if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenHint), xmlszLocalname))
	{
		// install a parse handler for the hint config
		CParseHandlerBase *pphHint = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenHint), m_pphm, this);
		m_pphm->ActivateParseHandler(pphHint);
		pphHint->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
		this->Append(pphHint);
		return;

	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenCostModelConfig), xmlszLocalname))
	{
		// install a parse handler for the cost model config
		CParseHandlerBase *pphCostModel = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenCostModelConfig), m_pphm, this);
		m_pphm->ActivateParseHandler(pphCostModel);
		pphCostModel->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
		this->Append(pphCostModel);
		return;

	}
	else if (0 == XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenTraceFlags), xmlszLocalname))
	{
		// install a parse handler for the trace flags
		CParseHandlerBase *pphTraceFlags = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenTraceFlags), m_pphm, this);
		m_pphm->ActivateParseHandler(pphTraceFlags);
		pphTraceFlags->startElement(xmlszUri, xmlszLocalname, xmlszQname, attrs);
		this->Append(pphTraceFlags);
		return;

	}
	else if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOptimizerConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE(gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}

	// install a parse handler for the CTE configuration
	CParseHandlerBase *pphCTEConfig = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenCTEConfig), m_pphm, this);
	m_pphm->ActivateParseHandler(pphCTEConfig);

	// install a parse handler for the statistics configuration
	CParseHandlerBase *pphStatisticsConfig = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenStatisticsConfig), m_pphm, this);
	m_pphm->ActivateParseHandler(pphStatisticsConfig);

	// install a parse handler for the enumerator configuration
	CParseHandlerBase *pphEnumeratorConfig = CParseHandlerFactory::Pph(m_pmp, CDXLTokens::XmlstrToken(EdxltokenEnumeratorConfig), m_pphm, this);
	m_pphm->ActivateParseHandler(pphEnumeratorConfig);

	// store parse handlers
	this->Append(pphEnumeratorConfig);
	this->Append(pphStatisticsConfig);
	this->Append(pphCTEConfig);
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOptimizerConfig::EndElement
//
//	@doc:
//		Invoked by Xerces to process a closing tag
//
//---------------------------------------------------------------------------
void
CParseHandlerOptimizerConfig::EndElement
	(
	const XMLCh* const, // xmlszUri,
	const XMLCh* const xmlszLocalname,
	const XMLCh* const // xmlszQname
	)
{
	if (0 != XMLString::compareString(CDXLTokens::XmlstrToken(EdxltokenOptimizerConfig), xmlszLocalname))
	{
		CWStringDynamic *pstr = CDXLUtils::PstrFromXMLCh(m_pphm->Pmm(), xmlszLocalname);
		GPOS_RAISE( gpdxl::ExmaDXL, gpdxl::ExmiDXLUnexpectedTag, pstr->Wsz());
	}
	
	GPOS_ASSERT(NULL == m_poconf);
	GPOS_ASSERT(6 >= this->UlLength());

	CParseHandlerEnumeratorConfig *pphEnumeratorConfig = dynamic_cast<CParseHandlerEnumeratorConfig *>((*this)[0]);
	CEnumeratorConfig *pec = pphEnumeratorConfig->Pec();
	pec->AddRef();

	CParseHandlerStatisticsConfig *pphStatisticsConfig = dynamic_cast<CParseHandlerStatisticsConfig *>((*this)[1]);
	CStatisticsConfig *pstatsconf = pphStatisticsConfig->Pstatsconf();
	pstatsconf->AddRef();

	CParseHandlerCTEConfig *pphCTEConfig = dynamic_cast<CParseHandlerCTEConfig *>((*this)[2]);
	CCTEConfig *pcteconfig = pphCTEConfig->Pcteconf();
	pcteconfig->AddRef();
	
	ICostModel *pcm = NULL;
	CHint *phint = NULL;
	if (5 == this->UlLength())
	{
		CParseHandlerCostModel *pphCostModelConfig = dynamic_cast<CParseHandlerCostModel *>((*this)[3]);
		pcm = pphCostModelConfig->Pcm();
		GPOS_ASSERT(NULL != pcm);
		pcm->AddRef();

		phint = CHint::PhintDefault(m_pmp);
	}
	else if (6 == this->UlLength())
	{
		CParseHandlerCostModel *pphCostModelConfig = dynamic_cast<CParseHandlerCostModel *>((*this)[3]);
		pcm = pphCostModelConfig->Pcm();
		GPOS_ASSERT(NULL != pcm);
		pcm->AddRef();

		CParseHandlerHint *pphHint = dynamic_cast<CParseHandlerHint *>((*this)[4]);
		phint = pphHint->Phint();
		GPOS_ASSERT(NULL != phint);
		phint->AddRef();
	}
	else
	{
		// no cost model: use default one
		pcm = ICostModel::PcmDefault(m_pmp);
		phint = CHint::PhintDefault(m_pmp);
	}

	m_poconf = GPOS_NEW(m_pmp) COptimizerConfig(pec, pstatsconf, pcteconfig, pcm, phint);

	CParseHandlerTraceFlags *pphTraceFlags = dynamic_cast<CParseHandlerTraceFlags *>((*this)[this->UlLength() - 1]);
	pphTraceFlags->Pbs()->AddRef();
	m_pbs = pphTraceFlags->Pbs();
	
	// deactivate handler
	m_pphm->DeactivateHandler();
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOptimizerConfig::Edxlphtype
//
//	@doc:
//		Return the type of the parse handler.
//
//---------------------------------------------------------------------------
EDxlParseHandlerType
CParseHandlerOptimizerConfig::Edxlphtype() const
{
	return EdxlphOptConfig;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOptimizerConfig::Pbs
//
//	@doc:
//		Returns the bitset for the trace flags
//
//---------------------------------------------------------------------------
CBitSet *
CParseHandlerOptimizerConfig::Pbs() const
{
	return m_pbs;
}

//---------------------------------------------------------------------------
//	@function:
//		CParseHandlerOptimizerConfig::Poc
//
//	@doc:
//		Returns the optimizer config
//
//---------------------------------------------------------------------------
COptimizerConfig *
CParseHandlerOptimizerConfig::Poconf() const
{
	return m_poconf;
}

// EOF
