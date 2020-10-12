//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CDistributionSpecReplicated.h
//
//	@doc:
//		Description of a replicated distribution;
//		Can be used as required or derived property;
//---------------------------------------------------------------------------
#ifndef GPOPT_CDistributionSpecReplicated_H
#define GPOPT_CDistributionSpecReplicated_H

#include "gpos/base.h"

#include "gpopt/base/CDistributionSpec.h"
#include "gpopt/base/CDistributionSpecSingleton.h"

namespace gpopt
{
using namespace gpos;

//---------------------------------------------------------------------------
//	@class:
//		CDistributionSpecReplicated
//
//	@doc:
//		Class for representing replicated distribution specification.
//
//---------------------------------------------------------------------------
class CDistributionSpecReplicated : public CDistributionSpec
{
private:
public:
	CDistributionSpecReplicated(const CDistributionSpecReplicated &) = delete;

	// ctor
	CDistributionSpecReplicated() = default;

	// accessor
	EDistributionType
	Edt() const override
	{
		return CDistributionSpec::EdtReplicated;
	}

	// does this distribution satisfy the given one
	BOOL FSatisfies(const CDistributionSpec *pds) const override;

	// append enforcers to dynamic array for the given plan properties
	void AppendEnforcers(CMemoryPool *mp, CExpressionHandle &exprhdl,
						 CReqdPropPlan *prpp, CExpressionArray *pdrgpexpr,
						 CExpression *pexpr) override;

	// return distribution partitioning type
	EDistributionPartitioningType
	Edpt() const override
	{
		return EdptNonPartitioned;
	}

	// print
	IOstream &
	OsPrint(IOstream &os) const override
	{
		return os << "REPLICATED ";
	}

	// conversion function
	static CDistributionSpecReplicated *
	PdsConvert(CDistributionSpec *pds)
	{
		GPOS_ASSERT(NULL != pds);
		GPOS_ASSERT(EdtReplicated == pds->Edt());

		return dynamic_cast<CDistributionSpecReplicated *>(pds);
	}

};	// class CDistributionSpecReplicated

}  // namespace gpopt

#endif	// !GPOPT_CDistributionSpecReplicated_H

// EOF
