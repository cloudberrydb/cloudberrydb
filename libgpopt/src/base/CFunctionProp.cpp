//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CFunctionProp.cpp
//
//	@doc:
//		Implementation of function properties
//---------------------------------------------------------------------------

#include "gpos/base.h"

#include "gpopt/base/CFunctionProp.h"

using namespace gpopt;

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::CFunctionProp
//
//	@doc:
//		Ctor
//
//---------------------------------------------------------------------------
CFunctionProp::CFunctionProp
	(
	IMDFunction::EFuncStbl efsStability,
	IMDFunction::EFuncDataAcc efdaDataAccess,
	BOOL fHasVolatileFunctionScan,
	BOOL fScan
	)
	:
	m_efs(efsStability),
	m_efda(efdaDataAccess),
	m_fHasVolatileFunctionScan(fHasVolatileFunctionScan),
	m_fScan(fScan)
{
	GPOS_ASSERT(IMDFunction::EfsSentinel > efsStability);
	GPOS_ASSERT(IMDFunction::EfdaSentinel > efdaDataAccess);
	GPOS_ASSERT_IMP(fScan && IMDFunction::EfsVolatile == efsStability, fHasVolatileFunctionScan);
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::~CFunctionProp
//
//	@doc:
//		Dtor
//
//---------------------------------------------------------------------------
CFunctionProp::~CFunctionProp()
{
}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::FMasterOnly
//
//	@doc:
//		Check if must execute on the master based on function properties
//
//---------------------------------------------------------------------------
BOOL
CFunctionProp::FMasterOnly() const
{
	// a function needs to execute on the master if any of the following holds:
	// a) it reads or modifies SQL data
	// b) it is volatile and used as a scan operator (i.e. in the from clause)

	// TODO:  - Feb 10, 2014; enable the following line instead of the
	// current return statement once all function properties are fixed
	//return (IMDFunction::EfdaContainsSQL < m_efda || (m_fScan && IMDFunction::EfsVolatile == m_efs));

	return m_fScan && (IMDFunction::EfsVolatile == m_efs || IMDFunction::EfsStable == m_efs);

}

//---------------------------------------------------------------------------
//	@function:
//		CFunctionProp::OsPrint
//
//	@doc:
//		debug print
//
//---------------------------------------------------------------------------
IOstream &
CFunctionProp::OsPrint
	(
	IOstream &os
	)
	const
{
	const CHAR *rgszStability[] = {"Immutable",	"Stable", "Volatile"};
	const CHAR *rgszDataAccess[] = {"NoSQL", "ContainsSQL",	"ReadsSQLData",	"ModifiesSQLData"};

	os << rgszStability[m_efs] << ", " << rgszDataAccess[m_efda];
	return os;
}

// EOF
