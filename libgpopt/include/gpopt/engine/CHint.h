//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2016 Pivotal Software, Inc.
//
//	@filename:
//		CHint.h
//
//	@doc:
//		Hint configurations
//
//	@owner:
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------
#ifndef GPOPT_CHint_H
#define GPOPT_CHint_H

#include "gpos/base.h"
#include "gpos/memory/IMemoryPool.h"
#include "gpos/common/CRefCount.h"

namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CHint
	//
	//	@doc:
	//		Hint configurations
	//
	//---------------------------------------------------------------------------
	class CHint : public CRefCount
	{

		private:

			ULONG m_ulMinNumOfPartsToRequireSortOnInsert;

			ULONG m_ulJoinArityForAssociativityCommutativity;

			ULONG m_ulArrayExpansionThreshold;


			// private copy ctor
			CHint(const CHint &);

		public:

			// ctor
			CHint
				(
				ULONG ulMinNumOfPartsToRequireSortOnInsert,
				ULONG ulJoinArityForAssociativityCommutativity,
				ULONG ulArrayExpansionThreshold
				)
				:
				m_ulMinNumOfPartsToRequireSortOnInsert(ulMinNumOfPartsToRequireSortOnInsert),
				m_ulJoinArityForAssociativityCommutativity(ulJoinArityForAssociativityCommutativity),
				m_ulArrayExpansionThreshold(ulArrayExpansionThreshold)
			{
			}


			// Minimum number of partitions required for sorting tuples during
			// insertion in an append only row-oriented partitioned table
			ULONG UlMinNumOfPartsToRequireSortOnInsert() const
			{
				return m_ulMinNumOfPartsToRequireSortOnInsert;
			}

			// Maximum number of relations in an n-ary join operator where ORCA will
			// explore JoinAssociativity and JoinCommutativity transformations.
			// When the number of relations exceed this we'll prune the search space
			// by not pursuing the above mentioned two transformations.
			ULONG UlJoinArityForAssociativityCommutativity() const
			{
				return m_ulJoinArityForAssociativityCommutativity;
			}

			// Maximum number of elements in the scalar comparison with an array which
			// will be expanded during constraint derivation. The benefits of using a smaller number
			// are avoiding expensive expansion of constraints in terms of memory and optimization
			// time
			ULONG UlArrayExpansionThreshold() const
			{
				return m_ulArrayExpansionThreshold;
			}

			// generate default hint configurations, which disables sort during insert on
			// append only row-oriented partitioned tables by default
			static
			CHint *PhintDefault(IMemoryPool *pmp)
			{
				return GPOS_NEW(pmp) CHint(INT_MAX /* ulMinNumOfPartsToRequireSortOnInsert */,
										   INT_MAX /* ulJoinArityForAssociativityCommutativity */,
										   INT_MAX /* ulArrayExpansionThreshold */);
			}

	}; // class CHint
}

#endif // !GPOPT_CHint_H

// EOF
