//	Greenplum Database
//	Copyright (C) 2018 Pivotal Software, Inc.

#include "naucrates/dxl/CCostModelConfigSerializer.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "gpdbcost/CCostModelParamsGPDB.h"

#include "gpos/common/CAutoRef.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

using namespace gpdxl;
using gpos::CAutoRef;

void CCostModelConfigSerializer::Serialize(CXMLSerializer &xmlser) const
{
	if (ICostModel::EcmtGPDBLegacy == m_pcm->Ecmt())
	{
		return;
	}

	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostModelConfig));
	xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenCostModelType), m_pcm->Ecmt());
	xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenSegmentsForCosting), m_pcm->UlHosts());

	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostParams));

	xmlser.OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostParam));

	xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenName), m_pcm->Pcp()->SzNameLookup(CCostModelParamsGPDB::EcpNLJFactor));
	xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenValue), m_pcm->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor)->DVal());
	xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenCostParamLowerBound), m_pcm->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor)->DLowerBound());
	xmlser.AddAttribute(CDXLTokens::PstrToken(EdxltokenCostParamUpperBound), m_pcm->Pcp()->PcpLookup(CCostModelParamsGPDB::EcpNLJFactor)->DUpperBound());
	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostParam));

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostParams));

	xmlser.CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix), CDXLTokens::PstrToken(EdxltokenCostModelConfig));
}

CCostModelConfigSerializer::CCostModelConfigSerializer
	(
	const gpopt::ICostModel *pcm
	)
	:
	m_pcm(pcm)
{
}
