//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerFactory.h
//
//	@doc:
//		Factory methods for creating SAX parse handlers
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerFactory_H
#define GPDXL_CParseHandlerFactory_H

#include "gpos/base.h"
#include "gpos/common/CHashMap.h"

#include "naucrates/exception.h"
#include "naucrates/dxl/operators/CDXLPhysical.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
	using namespace gpos;
	
	XERCES_CPP_NAMESPACE_USE

	// shorthand for functions creating operator parse handlers 
	typedef CParseHandlerBase* (PfParseHandlerOpCreator) (IMemoryPool *pmp, CParseHandlerManager *, CParseHandlerBase *);
	
	// fwd decl
	class CDXLTokens;
	
	const ULONG ulHashMapSize = 128;
	
	// function for hashing xerces strings
	inline 
	ULONG UlHashXMLStr(const XMLCh *xmlsz)
	{
		return (ULONG) XMLString::hash(xmlsz, ulHashMapSize);
	}
	
	// function for equality on xerces strings
	inline 
	BOOL FEqualXMLStr(const XMLCh *xmlsz1, const XMLCh *xmlsz2)
	{
		return (0 == XMLString::compareString(xmlsz1, xmlsz2));
	}

	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerFactory
	//
	//	@doc:
	//		Factory class for creating DXL SAX parse handlers
	//
	//---------------------------------------------------------------------------
	class CParseHandlerFactory
	{
		
		typedef CHashMap<const XMLCh, PfParseHandlerOpCreator, UlHashXMLStr, FEqualXMLStr,
			CleanupNULL, CleanupNULL > HMXMLStrPfPHCreator;

		// pair of DXL token type and the corresponding parse handler
		struct SParseHandlerMapping
		{
			// type
			Edxltoken edxltoken;

			// translator function pointer
			PfParseHandlerOpCreator *pfphopc;
		};
		
		private:
			// mappings DXL token -> ParseHandler creator
			static 
			HMXMLStrPfPHCreator *m_phmPHCreators;

			static 
			void AddMapping(Edxltoken edxltok, PfParseHandlerOpCreator *pfphopc);
						
			// construct a physical op parse handlers
			static
			CParseHandlerBase *PphPhysOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a GPDB plan parse handler
			static
			CParseHandlerBase *PphPlan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a metadata parse handler
			static
			CParseHandlerBase *PphMetadata
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a metadata request parse handler
			static
			CParseHandlerBase *PphMDRequest
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);
			
			// construct a parse handler for the optimizer configuration
			static 
			CParseHandlerBase *PphOptimizerConfig
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a parse handler for the enumerator configuration
			static
			CParseHandlerBase *PphEnumeratorConfig
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a parse handler for the statistics configuration
			static
			CParseHandlerBase *PphStatisticsConfig
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a parse handler for the CTE configuration
			static
			CParseHandlerBase *PphCTEConfig
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a parse handler for the cost model configuration
			static
			CParseHandlerBase *PphCostModelConfig
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct hint parse handler
			static
			CParseHandlerBase *PphHint
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct window oids parse handler
			static
			CParseHandlerBase *PphWindowOids
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a trace flag parse handler
			static 
			CParseHandlerBase *PphTraceFlags
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a MD relation parse handler
			static 
			CParseHandlerBase *PphMetadataRelation
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a MD external relation parse handler
			static
			CParseHandlerBase *PphMetadataRelationExternal
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a MD CTAS relation parse handler
			static
			CParseHandlerBase *PphMetadataRelationCTAS
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an MD index parse handler
			static 
			CParseHandlerBase *PphMDIndex
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a relation stats parse handler
			static 
			CParseHandlerBase *PphRelStats
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a column stats parse handler
			static 
			CParseHandlerBase *PphColStats
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a column stats bucket parse handler
			static 
			CParseHandlerBase *PphColStatsBucket
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an MD type parse handler
			static
			CParseHandlerBase *PphMDGPDBType
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an MD scalarop parse handler
			static
			CParseHandlerBase *PphMDGPDBScalarOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an MD function parse handler
			static
			CParseHandlerBase *PphMDGPDBFunc
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an MD aggregate operation parse handler
			static
			CParseHandlerBase *PphMDGPDBAgg
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an MD trigger parse handler
			static
			CParseHandlerBase *PphMDGPDBTrigger
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an MD cast parse handler
			static
			CParseHandlerBase *PphMDCast
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an MD scalar comparison parse handler
			static
			CParseHandlerBase *PphMDScCmp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an MD check constraint parse handler
			static
			CParseHandlerBase *PphMDGPDBCheckConstraint
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a parse handler for a list of MD ids
			static
			CParseHandlerBase *PphMetadataIdList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a metadata columns parse handler
			static
			CParseHandlerBase *PphMetadataColumns
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			static
			CParseHandlerBase * PphMDIndexInfoList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a column MD parse handler
			static
			CParseHandlerBase *PphMetadataColumn
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a column default value expression parse handler
			static
			CParseHandlerBase *PphColumnDefaultValueExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a scalar operator parse handler
			static
			CParseHandlerBase *PphScalarOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a properties parse handler
			static
			CParseHandlerBase *PphProperties
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a filter operator parse handler
			static
			CParseHandlerBase *PphFilter
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a table scan parse handler
			static
			CParseHandlerBase *PphTableScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a bitmap table scan parse handler
			static
			CParseHandlerBase *PphBitmapTableScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a dynamic bitmap table scan parse handler
			static
			CParseHandlerBase *PphDynamicBitmapTableScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an external scan parse handler
			static
			CParseHandlerBase *PphExternalScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a subquery scan parse handler
			static
			CParseHandlerBase *PphSubqScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a result node parse handler
			static
			CParseHandlerBase *PphResult
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a HJ parse handler
			static
			CParseHandlerBase *PphHashJoin
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a NLJ parse handler
			static
			CParseHandlerBase *PphNLJoin
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a merge join parse handler
			static
			CParseHandlerBase *PphMergeJoin
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a sort parse handler
			static
			CParseHandlerBase *PphSort
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an append parse handler
			static
			CParseHandlerBase *PphAppend
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a materialize parse handler
			static
			CParseHandlerBase *PphMaterialize
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a dynamic table scan parse handler
			static
			CParseHandlerBase *PphDynamicTableScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a dynamic index scan parse handler
			static
			CParseHandlerBase *PphDynamicIndexScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a partition selector parse handler
			static
			CParseHandlerBase *PphPartitionSelector
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a sequence parse handler
			static
			CParseHandlerBase *PphSequence
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a limit (physical) parse handler
			static
			CParseHandlerBase *PphLimit
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a limit count parse handler
			static
			CParseHandlerBase *PphLimitcount
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a limit offset parse handler
			static
			CParseHandlerBase *PphLimitoffset
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a subquery parse handler
			static
			CParseHandlerBase *PphScalarSubquery
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a subquery parse handler
			static
			CParseHandlerBase *PphScalarBitmapBoolOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an array parse handler
			static
			CParseHandlerBase *PphScalarArray
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct an arrayref parse handler
			static
			CParseHandlerBase *PphScalarArrayRef
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct an arrayref index list parse handler
			static
			CParseHandlerBase *PphScalarArrayRefIndexList
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct an assert predicate parse handler
			static
			CParseHandlerBase *PphScalarAssertConstraintList
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);


			// construct a DML action parse handler
			static
			CParseHandlerBase *PphScalarDMLAction
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);
			
			// construct a scalar operator list
			static
			CParseHandlerBase *PphScalarOpList
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct a scalar part oid
			static
			CParseHandlerBase *PphScalarPartOid
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct a scalar part default
			static
			CParseHandlerBase *PphScalarPartDefault
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct a scalar part bound
			static
			CParseHandlerBase *PphScalarPartBound
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct a scalar part bound inclusion
			static
			CParseHandlerBase *PphScalarPartBoundInclusion
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct a scalar part bound openness
			static
			CParseHandlerBase *PphScalarPartBoundOpen
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct a scalar part list values
			static
			CParseHandlerBase *PphScalarPartListValues
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct a scalar part list null test
			static
			CParseHandlerBase *PphScalarPartListNullTest
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);

			// construct a direct dispatch info parse handler
			static
			CParseHandlerBase *PphDirectDispatchInfo
				(
				IMemoryPool* pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);
			
			// construct a gather motion parse handler
			static
			CParseHandlerBase *PphGatherMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a broadcast motion parse handler
			static
			CParseHandlerBase *PphBroadcastMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a redistribute motion parse handler
			static
			CParseHandlerBase *PphRedistributeMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a routed motion parse handler
			static
			CParseHandlerBase *PphRoutedMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a random motion parse handler
			static
			CParseHandlerBase *PphRandomMotion
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a physical aggregate parse handler
			static
			CParseHandlerBase *PphAgg
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an aggregate function parse handler
			static
			CParseHandlerBase *PphAggref
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a parse handler for a physical window node
			static
			CParseHandlerBase *PphWindow
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an window function parse handler
			static
			CParseHandlerBase *PphWindowRef
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an window frame parse handler
			static
			CParseHandlerBase *PphWindowFrame
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an window key parse handler
			static
			CParseHandlerBase *PphWindowKey
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a parse handler to parse the list of window keys
			static
			CParseHandlerBase *PphWindowKeyList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an window specification parse handler
			static
			CParseHandlerBase *PphWindowSpec
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a parse handler to parse the list of window specifications
			static
			CParseHandlerBase *PphWindowSpecList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a grouping column list parse handler
			static
			CParseHandlerBase *PphGroupingColList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a comparison operator parse handler
			static
			CParseHandlerBase *PphScalarCmp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a distinct compare parse handler
			static
			CParseHandlerBase *PphDistinctCmp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a scalar identifier parse handler
			static
			CParseHandlerBase *PphScalarId
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a scalar operator parse handler
			static
			CParseHandlerBase *PphScalarOpexpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an array compare parse handler
			static
			CParseHandlerBase *PphScalarArrayCmp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a boolean expression parse handler
			static
			CParseHandlerBase *PphScalarBoolExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a min/max parse handler
			static
			CParseHandlerBase *PphScalarMinMax
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a boolean test parse handler
			static
			CParseHandlerBase *PphBooleanTest
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a null test parse handler
			static
			CParseHandlerBase *PphScalarNullTest
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a nullif parse handler
			static
			CParseHandlerBase *PphScalarNullIf
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a cast parse handler
			static
			CParseHandlerBase *PphScalarCast
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a coerce parse handler
			static
			CParseHandlerBase *PphScalarCoerceToDomain
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a coerceviaio parse handler
			static
			CParseHandlerBase *PphScalarCoerceViaIO
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a ArrayCoerceExpr parse handler
			static
			CParseHandlerBase *PphScalarArrayCoerceExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a sub plan parse handler
			static
			CParseHandlerBase *PphScalarSubPlan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// create a parse handler for parsing a SubPlan test expression
			static
			CParseHandlerBase *PphScalarSubPlanTestExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a sub plan params parse handler
			static
			CParseHandlerBase *PphScalarSubPlanParamList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a sub plan param parse handler
			static
			CParseHandlerBase *PphScalarSubPlanParam
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical TVF parse handler
			static
			CParseHandlerBase *PphLogicalTVF
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a physical TVF parse handler
			static
			CParseHandlerBase *PphPhysicalTVF
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a coalesce parse handler
			static
			CParseHandlerBase *PphScalarCoalesce
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a switch parse handler
			static
			CParseHandlerBase *PphScalarSwitch
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a switch case parse handler
			static
			CParseHandlerBase *PphScalarSwitchCase
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a case test parse handler
			static
			CParseHandlerBase *PphScalarCaseTest
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a constant parse handler
			static
			CParseHandlerBase *PphScalarConstValue
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an if statement parse handler
			static
			CParseHandlerBase *PphIfStmt
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a function parse handler
			static
			CParseHandlerBase *PphScalarFuncExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a project list parse handler
			static
			CParseHandlerBase *PphProjList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a project element parse handler
			static
			CParseHandlerBase *PphProjElem
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a hash expression list parse handler
			static
			CParseHandlerBase *PphHashExprList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);		
			
			// construct a hash expression parse handler
			static
			CParseHandlerBase *PphHashExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a condition list parse handler
			static
			CParseHandlerBase *PphCondList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a sort column list parse handler
			static
			CParseHandlerBase *PphSortColList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a sort column parse handler
			static
			CParseHandlerBase *PphSortCol
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a cost parse handler
			static
			CParseHandlerBase *PphCost
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a table descriptor parse handler
			static
			CParseHandlerBase *PphTableDesc
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a column descriptor parse handler
			static
			CParseHandlerBase *PphColDesc
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct an index scan list parse handler
			static
			CParseHandlerBase *PphIndexScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an index only scan parse handler
			static
			CParseHandlerBase *PphIndexOnlyScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a bitmap index scan list parse handler
			static
			CParseHandlerBase *PphBitmapIndexProbe
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an index descriptor list parse handler
			static
			CParseHandlerBase *PphIndexDescr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct an index condition list parse handler
			static
			CParseHandlerBase *PphIndexCondList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);


			// construct a query parse handler
			static
			CParseHandlerBase *PphQuery
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical get parse handler
			static
			CParseHandlerBase *PphLgGet
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical external get parse handler
			static
			CParseHandlerBase *PphLgExternalGet
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical operator parse handler
			static
			CParseHandlerBase *PphLgOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical project parse handler
			static
			CParseHandlerBase *PphLgProject
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical CTE producer parse handler
			static
			CParseHandlerBase *PphLgCTEProducer
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a logical CTE consumer parse handler
			static
			CParseHandlerBase *PphLgCTEConsumer
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a logical CTE anchor parse handler
			static
			CParseHandlerBase *PphLgCTEAnchor
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a CTE list
			static
			CParseHandlerBase *PphCTEList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a logical window parse handler
			static
			CParseHandlerBase *PphLgWindow
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical insert parse handler
			static
			CParseHandlerBase *PphLgInsert
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a logical delete parse handler
			static
			CParseHandlerBase *PphLgDelete
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical update parse handler
			static
			CParseHandlerBase *PphLgUpdate
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a logical CTAS parse handler
			static
			CParseHandlerBase *PphLgCTAS
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a physical CTAS parse handler
			static
			CParseHandlerBase *PphPhCTAS
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a parse handler for parsing CTAS storage options
			static
			CParseHandlerBase *PphCTASOptions
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a physical CTE producer parse handler
			static
			CParseHandlerBase *PphPhCTEProducer
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a physical CTE consumer parse handler
			static
			CParseHandlerBase *PphPhCTEConsumer
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a physical DML parse handler
			static
			CParseHandlerBase *PphPhDML
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a physical split parse handler
			static
			CParseHandlerBase *PphPhSplit
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a physical row trigger parse handler
			static
			CParseHandlerBase *PphPhRowTrigger
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a physical assert parse handler
			static
			CParseHandlerBase *PphPhAssert
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a logical set operator parse handler
			static
			CParseHandlerBase *PphLgSetOp
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical select parse handler
			static
			CParseHandlerBase *PphLgSelect
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical join parse handler
			static
			CParseHandlerBase *PphLgJoin
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical query output parse handler
			static
			CParseHandlerBase *PphQueryOutput
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a logical groupby parse handler
			static
			CParseHandlerBase *PphLgGrpBy
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);	

			// construct a logical limit parse handler
			static
			CParseHandlerBase *PphLgLimit
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a logical const table parse handler
			static
			CParseHandlerBase *PphLgConstTable
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a quantified subquery parse handler
			static
			CParseHandlerBase *PphScSubqueryQuantified
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	
			// construct a subquery parse handler
			static
			CParseHandlerBase *PphScSubqueryExists
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a pass-through parse handler for stack traces
			static
			CParseHandlerBase *PphStacktrace
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pph
				);
			
			// construct a statistics parse handler
			static
			CParseHandlerBase *PphStats
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a derived column parse handler
			static
			CParseHandlerBase *PphStatsDerivedColumn
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a derived relation stats parse handler
			static
			CParseHandlerBase *PphStatsDerivedRelation
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a bucket bound parse handler
			static
			CParseHandlerBase *PphStatsBucketBound
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// construct a trailing window frame edge parser
			static
			CParseHandlerBase *PphWindowFrameTrailingEdge
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a leading window frame edge parser
			static
			CParseHandlerBase *PphWindowFrameLeadingEdge
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct search strategy parse handler
			static
			CParseHandlerBase *PphSearchStrategy
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct search stage parse handler
			static
			CParseHandlerBase *PphSearchStage
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct xform parse handler
			static
			CParseHandlerBase *PphXform
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct cost params parse handler
			static
			CParseHandlerBase *PphCostParams
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct cost param parse handler
			static
			CParseHandlerBase *PphCostParam
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a scalar expression parse handler
			static
			CParseHandlerBase *PphScalarExpr
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a scalar values list parse handler
			static
			CParseHandlerBase *PphScalarValuesList
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a values scan parse handler
			static
			CParseHandlerBase *PphValuesScan
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// construct a values scan parse handler
			static
			CParseHandlerBase *PphMDArrayCoerceCast
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

		public:
			
			// initialize mappings of tokens to parse handlers
			static 
			void Init(IMemoryPool *pmp);
			
			// return the parse handler creator for operator with the given name
			static 
			CParseHandlerBase *Pph
				(
				IMemoryPool *pmp,
				const XMLCh *xmlsz,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// factory methods for creating parse handlers
			static 
			CParseHandlerDXL *Pphdxl
				(
				IMemoryPool *pmp,
				CParseHandlerManager*
				);
	};
}

#endif // !GPDXL_CParseHandlerFactory_H

// EOF
