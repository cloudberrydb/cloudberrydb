//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CEnfdPartitionPropagation.h
//
//	@doc:
//		Enforceable partition propagation property
//---------------------------------------------------------------------------
#ifndef GPOPT_CEnfdPartitionPropagation_H
#define GPOPT_CEnfdPartitionPropagation_H

#include "gpos/base.h"

#include "gpopt/base/CEnfdProp.h"
#include "gpopt/base/CPartitionPropagationSpec.h"


namespace gpopt
{
using namespace gpos;


//---------------------------------------------------------------------------
//	@class:
//		CEnfdPartitionPropagation
//
//	@doc:
//		Enforceable distribution property;
//
//---------------------------------------------------------------------------
class CEnfdPartitionPropagation : public CEnfdProp
{
public:
	// type of partition matching function(s)
	enum EPartitionPropagationMatching
	{
		EppmSatisfy = 0,
		EppmSentinel
	};

private:
	// partition propagation spec
	CPartitionPropagationSpec *m_ppps;

	// partition propagation matching type
	EPartitionPropagationMatching m_eppm;


public:
	CEnfdPartitionPropagation(const CEnfdPartitionPropagation &) = delete;

	// ctor
	CEnfdPartitionPropagation(CPartitionPropagationSpec *ppps,
							  EPartitionPropagationMatching eppm);

	// dtor
	~CEnfdPartitionPropagation() override;

	// partition spec accessor
	CPropSpec *
	Pps() const override
	{
		return m_ppps;
	}

	// hash function
	ULONG HashValue() const override;

	// required propagation accessor
	CPartitionPropagationSpec *
	PppsRequired() const
	{
		return m_ppps;
	}

	// get distribution enforcing type for the given operator
	EPropEnforcingType Epet(CExpressionHandle &exprhdl, CPhysical *popPhysical,
							BOOL fPropagationReqd) const;

	// return matching type
	EPartitionPropagationMatching
	Eppm() const
	{
		return m_eppm;
	}

	// matching function
	BOOL Matches(CEnfdPartitionPropagation *pepp);

	BOOL FCompatible(CPartitionPropagationSpec *pps_drvd) const;

	// print function
	IOstream &OsPrint(IOstream &os) const override;

	// name of propagation matching type
	static const CHAR *SzPropagationMatching(
		EPartitionPropagationMatching eppm);

};	// class CEnfdPartitionPropagation

}  // namespace gpopt


#endif	// !GPOPT_CEnfdPartitionPropagation_H

// EOF
