//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2019 Pivotal Inc.
//
//	@filename:
//		CJoinOrderDPv2.h
//
//	@doc:
//		Dynamic programming-based join order generation
//---------------------------------------------------------------------------
#ifndef GPOPT_CJoinOrderDPv2_H
#define GPOPT_CJoinOrderDPv2_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"
#include "gpos/common/CBitSet.h"
#include "gpos/io/IOstream.h"
#include "gpopt/base/CUtils.h"
#include "gpopt/xforms/CJoinOrder.h"
#include "gpopt/operators/CExpression.h"


namespace gpopt
{
	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CJoinOrderDPv2
	//
	//	@doc:
	//		Helper class for creating join orders using dynamic programming
	//
	//---------------------------------------------------------------------------
	class CJoinOrderDPv2 : public CJoinOrder
	{

		private:

			//---------------------------------------------------------------------------
			//	@struct:
			//		SComponentPair
			//
			//	@doc:
			//		Struct containing a component, its best expression, and cost
			//
			//---------------------------------------------------------------------------
			struct SComponentInfo : public CRefCount
				{
					CBitSet *component;
					CExpression *best_expr;
					CDouble cost;

					SComponentInfo() : component(NULL),
									   best_expr(NULL),
									   cost(0.0)
					{
					}

					SComponentInfo(CBitSet *component,
								   CExpression *best_expr,
								   CDouble cost
								   ) : component(component),
									   best_expr(best_expr),
									   cost(cost)
					{
					}

					~SComponentInfo()
					{
						CRefCount::SafeRelease(component);
						CRefCount::SafeRelease(best_expr);
					}

				};

			// hashing function
			static
			ULONG UlHashBitSet
				(
				const CBitSet *pbs
				)
			{
				GPOS_ASSERT(NULL != pbs);

				return pbs->HashValue();
			}

			// equality function
			static
			BOOL FEqualBitSet
				(
				const CBitSet *pbsFst,
				const CBitSet *pbsSnd
				)
			{
				GPOS_ASSERT(NULL != pbsFst);
				GPOS_ASSERT(NULL != pbsSnd);

				return pbsFst->Equals(pbsSnd);
			}


			// hash map from bit set to expression array
			typedef CHashMap<CBitSet, CExpressionArray, UlHashBitSet, FEqualBitSet,
			CleanupRelease<CBitSet>, CleanupRelease<CExpressionArray> > BitSetToExpressionArrayMap;

			// hash map iter from bit set to expression array
			typedef CHashMapIter<CBitSet, CExpressionArray, UlHashBitSet, FEqualBitSet,
			CleanupRelease<CBitSet>, CleanupRelease<CExpressionArray> > BitSetToExpressionArrayMapIter;

			// dynamic array of SComponentInfos
			typedef CDynamicPtrArray<SComponentInfo, CleanupRelease<SComponentInfo> > ComponentInfoArray;

			// dynamic array of componentInfoArray, where each index represents the level
			typedef CDynamicPtrArray<ComponentInfoArray, CleanupRelease> ComponentInfoArrayLevels;

			// list of components, organized by level, main data structure for dynamic programming
			ComponentInfoArrayLevels *m_join_levels;

			// array of top-k join expression
			CExpressionArray *m_topKOrders;

			// array of top-k join costs
			CDoubleArray *m_topKCosts;

			// dummy expression to used for non-joinable components
			CExpression *m_pexprDummy;

			CMemoryPool *m_mp;

			// build expression linking given components
			CExpression *PexprBuildPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// extract predicate joining the two given sets
			CExpression *PexprPred(CBitSet *pbsFst, CBitSet *pbsSnd);

			// add given join order to best results
			void AddJoinOrderToTopK(CExpression *pexprJoin, CDouble dCost);

			// compute cost of given join expression
			CDouble DCost(CExpression *pexpr);

			// derive stats on given expression
			virtual
			void DeriveStats(CExpression *pexpr);

			// enumerate all possible joins between the components in join_pair_bitsets on the
			// left side and those in other_join_pair_bitsets on the right
			BitSetToExpressionArrayMap *SearchJoinOrders(ComponentInfoArray *join_pair_bitsets, ComponentInfoArray *other_join_pair_bitsets);

			// reduce a list of expressions per component down to the cheapest expression per component
			ComponentInfoArray *GetCheapestJoinExprForBitSet(BitSetToExpressionArrayMap *bit_exprarray_map);

			void AddJoinExprAlternativeForBitSet(CBitSet *join_bitset, CExpression *join_expr, BitSetToExpressionArrayMap *map);

			// create a CLogicalJoin and a CExpression to join two components
			CExpression *GetJoinExpr(SComponentInfo *left_child, SComponentInfo *right_child);

			void AddJoinExprFromMapToTopK(BitSetToExpressionArrayMap *bitset_joinexpr_map);

			BitSetToExpressionArrayMap *MergeJoinExprsForBitSet(BitSetToExpressionArrayMap *map, BitSetToExpressionArrayMap *other_map);

			void AddJoinExprsForBitSet(BitSetToExpressionArrayMap *result_map, BitSetToExpressionArrayMap *candidate_map);

			// enumerate bushy joins (joins where both children are also joins) of level "current_level"
			BitSetToExpressionArrayMap *SearchBushyJoinOrders(ULONG current_level);

			void AddExprs(const CExpressionArray *candidate_join_exprs, CExpressionArray *result_join_exprs);

		public:

			// ctor
			CJoinOrderDPv2
				(
				CMemoryPool *mp,
				CExpressionArray *pdrgpexprComponents,
				CExpressionArray *pdrgpexprConjuncts
				);

			// dtor
			virtual
			~CJoinOrderDPv2();

			// main handler
			virtual
			CExpression *PexprExpand();

			// best join orders
			CExpressionArray *PdrgpexprTopK() const
			{
				return m_topKOrders;
			}

			// print function
			virtual
			IOstream &OsPrint(IOstream &) const;

	}; // class CJoinOrderDPv2

}

#endif // !GPOPT_CJoinOrderDPv2_H

// EOF
