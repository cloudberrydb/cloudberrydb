//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CTranslatorExprToDXLUtils.h
//
//	@doc:
//		Class providing helper methods for translating from Expr to DXL 
//---------------------------------------------------------------------------
#ifndef GPOPT_CTranslatorExprToDXLUtils_H
#define GPOPT_CTranslatorExprToDXLUtils_H

#include "gpos/base.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/operators/CDXLScalarBoolExpr.h"
#include "naucrates/dxl/operators/CDXLColDescr.h"

#include "gpopt/metadata/CTableDescriptor.h"
#include "gpopt/mdcache/CMDAccessor.h"
#include "gpopt/operators/CExpression.h"
#include "gpopt/operators/ops.h"

#include "gpopt/translate/CTranslatorExprToDXL.h"

// fwd decl
namespace gpmd
{
	class IMDRelation;
}

namespace gpdxl
{
	class CDXLPhysicalProperties;
	class CDXLScalarProjElem;
}

namespace gpopt
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpdxl;
	using namespace gpnaucrates;

	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorExprToDXLUtils
	//
	//	@doc:
	//		Class providing helper methods for translating from Expr to DXL
	//
	//---------------------------------------------------------------------------
	class CTranslatorExprToDXLUtils
	{
		private:
			// construct a scalar comparison of the given type between the 
			// column with the given col id and the scalar expression 
			static
			CDXLNode *PdxlnCmp
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				ULONG ulPartLevel,
				BOOL fLowerBound,
				CDXLNode *pdxlnScalar, 
				IMDType::ECmpType cmp_type, 
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeExpr,
				IMDId *pmdidTypeCastExpr,
				IMDId *mdid_cast_func
				);
			
			
			// create a column reference
			static
			CColRef *PcrCreate
				(
				IMemoryPool *mp,
				CMDAccessor *md_accessor,
				CColumnFactory *col_factory,
				IMDId *mdid,
				INT type_modifier,
				const WCHAR *wszName
				);

			// find the partitioning level of the given part key, given the whole
			// array of part keys
			static
			ULONG UlPartKeyLevel(const CColRef *colref, CColRef2dArray *pdrgpdrgpcr);

			// construct a test for a partial scan given a part constraint
			static 
			CDXLNode *PdxlnPartialScanTest
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CColumnFactory *col_factory,
				CConstraint *pcnstr,
				CColRef2dArray *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for a conjunction or disjunction-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestConjDisj
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CColumnFactory *col_factory,
				CConstraintArray *pdrgpcnstr,
				BOOL fConjunction,
				CColRef2dArray *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for a conjunction-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestConjunction
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CColumnFactory *col_factory,
				CConstraint *pcnstr,
				CColRef2dArray *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for a disjunction-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestDisjunction
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CColumnFactory *col_factory,
				CConstraint *pcnstr,
				CColRef2dArray *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for a negation-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestNegation
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CColumnFactory *col_factory,
				CConstraint *pcnstr,
				CColRef2dArray *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);
			
			// construct a test for an interval-based part constraint
			static 
			CDXLNode *PdxlnPartialScanTestInterval
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CConstraint *pcnstr,
				CColRef2dArray *pdrgpdrgpcrPartKeys,
				BOOL fRangePart
				);

			// construct a test for a range in a part constraint
			static 
			CDXLNode *PdxlnPartialScanTestRange
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CRange *prng,
				IMDId *pmdidPartKeyType,
				ULONG ulPartLevel,
				BOOL fRangePart
				);
			
			// construct a test for testing range containment with respect to the
			// start of the range
			static
			CDXLNode *PdxlnRangeStartPredicate
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				IDatum *datum,
				CRange::ERangeInclusion eri,
				IMDId *pmdidPartKeyType,
				ULONG ulPartLevel
				);
			
			
			// construct a test for testing range containment with respect to the
			// end of the range
			static
			CDXLNode *PdxlnRangeEndPredicate
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				IDatum *datum,
				CRange::ERangeInclusion eri,
				IMDId *pmdidPartKeyType,
				ULONG ulPartLevel
				);
			
			// construct a test for testing range containment with respect to the
			// given point in the range using the provided inclusion (<= or >=) 
			// and exclusion comparison operators (< or >)
			static
			CDXLNode *PdxlnRangePointPredicate
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				IDatum *datum,
				CRange::ERangeInclusion eri,
				IMDId *pmdidPartKeyType,
				IMDId *pmdidCmpExcl, 
				IMDId *pmdidCmpIncl,
				ULONG ulPartLevel,
				BOOL is_lower_bound
				);
			
			// construct a test for the default partition
			static
			CDXLNode *PdxlnDefaultPartitionTest
				(
				IMemoryPool *mp, 
				ULONG ulPartLevel
				);
			
			// compute a DXL datum from a point constraint
			static
			CDXLDatum *PdxldatumFromPointConstraint
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor,
				const CColRef *pcrDistrCol, 
				CConstraint *pcnstrDistrCol
				);
			
			// compute an array of DXL datum arrays from a disjunction of point constraints
			static
			CDXLDatum2dArray *PdrgpdrgpdxldatumFromDisjPointConstraint
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor,
				const CColRef *pcrDistrCol, 
				CConstraint *pcnstrDistrCol
				);
			
			// compute the direct dispatch info  from the constraints
			// on the distribution keys
			static
			CDXLDirectDispatchInfo *GetDXLDirectDispatchInfo
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor,
				CExpressionArray *pdrgpexprHashed, 
				CConstraint *pcnstr
				);
			
			// compute the direct dispatch info for a single distribution key from the constraints
			// on the distribution key
			static
			CDXLDirectDispatchInfo *PdxlddinfoSingleDistrKey
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor,
				CExpression *pexprHashed, 
				CConstraint *pcnstr
				);
			
			// check if the given constant value for a particular distribution column can be used
			// to identify which segment to direct dispatch to.
			static
			BOOL FDirectDispatchable(const CColRef *pcrDistrCol, const CDXLDatum *dxl_datum);

		public:
		
			// construct a default properties container
			static
			CDXLPhysicalProperties *GetProperties(IMemoryPool *mp);

			// create a scalar const value expression for the given bool value
			static
			CDXLNode *PdxlnBoolConst(IMemoryPool *mp, CMDAccessor *md_accessor, BOOL value);

			// create a scalar const value expression for the given int4 value
			static
			CDXLNode *PdxlnInt4Const(IMemoryPool *mp, CMDAccessor *md_accessor, INT val);

			// construct a filter node for a list partition predicate
			static
			CDXLNode *PdxlnListFilterScCmp
				(
				IMemoryPool *mp,
				CMDAccessor *md_accessor,
				CDXLNode *pdxlnPartKey,
				CDXLNode *pdxlnScalar,
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeOther,
				IMDType::ECmpType cmp_type,
				ULONG ulPartLevel,
				BOOL fHasDefaultPart
				);

			// construct a DXL node for the part key portion of the list partition filter
			static
			CDXLNode *PdxlnListFilterPartKey
				(
				IMemoryPool *mp,
				CMDAccessor *md_accessor,
				CExpression *pexprPartKey,
				IMDId *pmdidTypePartKey,
				ULONG ulPartLevel
				);

			// construct a filter node for a range predicate
			static
			CDXLNode *PdxlnRangeFilterScCmp
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CDXLNode *pdxlnScalar,
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeOther,
				IMDId *pmdidTypeCastExpr,
				IMDId *mdid_cast_func,
				IMDType::ECmpType cmp_type,
				ULONG ulPartLevel
				);
	
			// construct a range filter for an equality comparison
			static
			CDXLNode *PdxlnRangeFilterEqCmp
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CDXLNode *pdxlnScalar,
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeOther,
				IMDId *pmdidTypeCastExpr,
				IMDId *mdid_cast_func,
				ULONG ulPartLevel
				);
			
			// construct a predicate for the lower or upper bound of a partition
			static
			CDXLNode *PdxlnRangeFilterPartBound
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CDXLNode *pdxlnScalar,
				IMDId *pmdidTypePartKey,
				IMDId *pmdidTypeOther,
				IMDId *pmdidTypeCastExpr,
				IMDId *mdid_cast_func,
				ULONG ulPartLevel,
				ULONG fLowerBound,
				IMDType::ECmpType cmp_type
				);
			
			// construct predicates to cover the cases of default partition and
			// open-ended partitions if necessary
			static
			CDXLNode *PdxlnRangeFilterDefaultAndOpenEnded
				(
				IMemoryPool *mp, 
				ULONG ulPartLevel,
				BOOL fLTComparison,
				BOOL fGTComparison,
				BOOL fEQComparison,
				BOOL fDefaultPart
				);

			// construct a test for partial scan in the partial partition propagator
			static 
			CDXLNode *PdxlnPartialScanTest
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CColumnFactory *col_factory,
				const CPartConstraint *ppartcnstr,
				CColRef2dArray *pdrgpdrgpcrPartKeys,
				CharPtrArray *pdrgszPartTypes
				);
			
			// construct a nested if statement testing the constraints in the 
			// given part index map and propagating to the right part index id
			static
			CDXLNode *PdxlnPropagationExpressionForPartConstraints
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CColumnFactory *col_factory,
				UlongToPartConstraintMap *ppartcnstrmap,
				CColRef2dArray *pdrgpdrgpcrPartKeys,
				CharPtrArray *pdrgszPartTypes
				);
			
			// check if the DXL Node is a scalar const TRUE
			static
			BOOL FScalarConstTrue(CMDAccessor *md_accessor, CDXLNode *dxlnode);

			// check if the DXL Node is a scalar const false
			static
			BOOL FScalarConstFalse(CMDAccessor *md_accessor, CDXLNode *dxlnode);

			// check whether a project list has the same columns in the given array
			// and in the same order
			static
			BOOL FProjectListMatch(CDXLNode *pdxlnPrL, CColRefArray *colref_array);

			// create a project list by creating references to the columns of the given
			// project list of the child node
			static
			CDXLNode *PdxlnProjListFromChildProjList
				(
				IMemoryPool *mp, 
				CColumnFactory *col_factory, 
				ColRefToDXLNodeMap *phmcrdxln, 
				const CDXLNode *pdxlnProjListChild
				);

			// construct the project list of a partition selector
			static
			CDXLNode *PdxlnPrLPartitionSelector
				(
				IMemoryPool *mp,
				CMDAccessor *md_accessor,
				CColumnFactory *col_factory,
				ColRefToDXLNodeMap *phmcrdxln,
				BOOL fUseChildProjList,
				CDXLNode *pdxlnPrLChild,
				CColRef *pcrOid,
				ULONG ulPartLevels,
				BOOL fGeneratePartOid
				);

			// construct the propagation expression for a partition selector
			static
			CDXLNode *PdxlnPropExprPartitionSelector
				(
				IMemoryPool *mp,
				CMDAccessor *md_accessor,
				CColumnFactory *col_factory,
				BOOL fConditional,
				UlongToPartConstraintMap *ppartcnstrmap,
				CColRef2dArray *pdrgpdrgpcrKeys,
				ULONG scan_id,
				CharPtrArray *pdrgszPartTypes
				);

			// create a DXL project elem node from as a scalar identifier for the
			// child project element node
			static
			CDXLNode *PdxlnProjElem(IMemoryPool *mp, CColumnFactory *col_factory, ColRefToDXLNodeMap *phmcrdxln, const CDXLNode *pdxlnProjElemChild);
			
			// create a scalar identifier node for the given column reference
			static
			CDXLNode *PdxlnIdent(IMemoryPool *mp, ColRefToDXLNodeMap *phmcrdxlnSubplans, ColRefToDXLNodeMap *phmcrdxlnIndexLookup, const CColRef *colref);
			
			// replace subplan entry in the given map with a dxl column reference
			static
			void ReplaceSubplan(IMemoryPool *mp, ColRefToDXLNodeMap *phmcrdxlnSubplans, const CColRef *colref, CDXLScalarProjElem *pdxlopPrEl);

			// create a project elem from a given col ref
			static
			CDXLNode *PdxlnProjElem(IMemoryPool *mp, ColRefToDXLNodeMap *phmcrdxlnSubplans, const CColRef *colref);

			// construct an array of NULL datums for a given array of columns
			static
			IDatumArray *PdrgpdatumNulls(IMemoryPool *mp, CColRefArray *colref_array);

			// map an array of columns to a new array of columns
			static
			CColRefArray *PdrgpcrMapColumns(IMemoryPool *mp, CColRefArray *pdrgpcrInput, ColRefToUlongMap *phmcrul, CColRefArray *pdrgpcrMapDest);

			// combine two boolean expressions using the given boolean operator
			static
			CDXLNode *PdxlnCombineBoolean(IMemoryPool *mp, CDXLNode *first_child_dxlnode, CDXLNode *second_child_dxlnode, EdxlBoolExprType boolexptype);

			// construct a partition selector node
			static
			CDXLNode *PdxlnPartitionSelector
				(
				IMemoryPool *mp,
				IMDId *mdid,
				ULONG ulPartLevels,
				ULONG scan_id,
				CDXLPhysicalProperties *dxl_properties,
				CDXLNode *pdxlnPrL,
				CDXLNode *pdxlnEqFilters,
				CDXLNode *pdxlnFilters,
				CDXLNode *pdxlnResidual,
				CDXLNode *pdxlnPropagation,
				CDXLNode *pdxlnPrintable,
				CDXLNode *child_dxlnode = NULL
				);

			// create a DXL result node
			static
			CDXLNode *PdxlnResult
				(
				IMemoryPool *mp,
				CDXLPhysicalProperties *dxl_properties,
				CDXLNode *pdxlnPrL,
				CDXLNode *filter_dxlnode,
				CDXLNode *one_time_filter,
				CDXLNode *child_dxlnode
				);

			// create a DXL ValuesScan node
			static
			CDXLNode *PdxlnValuesScan
				(
				IMemoryPool *mp,
				CDXLPhysicalProperties *dxl_properties,
				CDXLNode *pdxlnPrL,
				IDatum2dArray *pdrgpdrgdatum
				);

			// build hashmap based on a column array, where the key is the column
			// and the value is the index of that column in the array
			static
			ColRefToUlongMap *PhmcrulColIndex(IMemoryPool *mp, CColRefArray *colref_array);
			
			// set statistics of the operator
			static
			void SetStats(IMemoryPool *mp, CMDAccessor *md_accessor, CDXLNode *dxlnode, const IStatistics *stats, BOOL fRoot);

			// set direct dispatch info of the operator
			static
			void SetDirectDispatchInfo
				(
				IMemoryPool *mp, 
				CMDAccessor *md_accessor, 
				CDXLNode *dxlnode, 
				CDrvdPropRelational *pdpRel, 
				CDistributionSpecArray *pdrgpdsBaseTables
				);
			
			// is the aggregate a local hash aggregate that is safe to stream
			static
			BOOL FLocalHashAggStreamSafe(CExpression *pexprAgg);
			
			// if operator is a scalar cast, extract cast type and function
			static 
			void ExtractCastMdids(COperator *pop, IMDId **ppmdidType, IMDId **ppmdidCastFunc);

			// produce DXL representation of a datum
			static
			CDXLDatum *GetDatumVal
				(
				IMemoryPool *mp,
				CMDAccessor *md_accessor,
				IDatum *datum
				)
			{
				IMDId *mdid = datum->MDId();
				return md_accessor->RetrieveType(mdid)->GetDatumVal(mp, datum);
			}

			// return a copy the dxl node's physical properties
			static
			CDXLPhysicalProperties *PdxlpropCopy(IMemoryPool *mp, CDXLNode *dxlnode);

			// check if given dxl operator exists in the given list
			static
			BOOL FDXLOpExists(const CDXLOperator *pop, const gpdxl::Edxlopid *peopid, ULONG ulOps);

			// check if given dxl node has any operator in the given list
			static
			BOOL FHasDXLOp(const CDXLNode *dxlnode, const gpdxl::Edxlopid *peopid, ULONG ulOps);

			// check if the project lists contains subplans with broadcast motion
			static
			BOOL FProjListContainsSubplanWithBroadCast(CDXLNode *pdxlnPrLNew);

			// check if the dxl node imposes a motion hazard
			static
			BOOL FMotionHazard(IMemoryPool *mp, CDXLNode *dxlnode, const gpdxl::Edxlopid *peopid, ULONG ulOps, CBitSet *pbsPrjCols);

			// check if the dxl operator does not impose a motion hazard
			static
			BOOL FMotionHazardSafeOp(CDXLNode *dxlnode);

			// extract the column ids of the ident from project list
			static
			void ExtractIdentColIds(CDXLNode *dxlnode, CBitSet *pbs);
	};
}

#endif // !GPOPT_CTranslatorExprToDXLUtils_H

// EOF
