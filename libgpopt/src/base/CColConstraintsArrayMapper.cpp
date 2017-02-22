//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.

#include "gpopt/base/CColConstraintsArrayMapper.h"
#include "gpopt/base/CConstraint.h"

using namespace gpopt;

DrgPcnstr *
CColConstraintsArrayMapper::PdrgPcnstrLookup
	(
		CColRef *pcr
	)
{
	const BOOL fExclusive = true;
	return CConstraint::PdrgpcnstrOnColumn(m_pmp, m_pdrgpcnstr, pcr, fExclusive);
}

CColConstraintsArrayMapper::CColConstraintsArrayMapper
	(
		gpos::IMemoryPool *pmp,
		DrgPcnstr *pdrgpcnstr
	) :
	m_pmp(pmp),
	m_pdrgpcnstr(pdrgpcnstr)
{
}

CColConstraintsArrayMapper::~CColConstraintsArrayMapper()
{
	m_pdrgpcnstr->Release();
}
