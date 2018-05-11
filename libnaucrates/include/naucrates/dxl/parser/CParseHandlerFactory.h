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
	typedef CParseHandlerBase* (ParseHandlerOpCreatorFunc) (IMemoryPool *mp, CParseHandlerManager *, CParseHandlerBase *);
	
	// fwd decl
	class CDXLTokens;
	
	const ULONG HASH_MAP_SIZE = 128;
	
	// function for hashing xerces strings
	inline 
	ULONG GetHashXMLStr(const XMLCh *xml_str)
	{
		return (ULONG) XMLString::hash(xml_str, HASH_MAP_SIZE);
	}
	
	// function for equality on xerces strings
	inline 
	BOOL IsXMLStrEqual(const XMLCh *xml_str1, const XMLCh *xml_str2)
	{
		return (0 == XMLString::compareString(xml_str1, xml_str2));
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
		
		typedef CHashMap<const XMLCh, ParseHandlerOpCreatorFunc, GetHashXMLStr, IsXMLStrEqual,
			CleanupNULL, CleanupNULL > TokenParseHandlerFuncMap;

		// pair of DXL token type and the corresponding parse handler
		struct SParseHandlerMapping
		{
			// type
			Edxltoken token_type;

			// translator function pointer
			ParseHandlerOpCreatorFunc *parse_handler_op_func;
		};
		
		private:
			// mappings DXL token -> ParseHandler creator
			static 
			TokenParseHandlerFuncMap *m_token_parse_handler_func_map;

			static 
			void AddMapping(Edxltoken token_type, ParseHandlerOpCreatorFunc *parse_handler_op_func);
						
			// construct a physical op parse handlers
			static
			CParseHandlerBase *CreatePhysicalOpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a GPDB plan parse handler
			static
			CParseHandlerBase *CreatePlanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a metadata parse handler
			static
			CParseHandlerBase *CreateMetadataParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a metadata request parse handler
			static
			CParseHandlerBase *CreateMDRequestParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *pph
				);
			
			// construct a parse handler for the optimizer configuration
			static 
			CParseHandlerBase *CreateOptimizerCfgParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a parse handler for the enumerator configuration
			static
			CParseHandlerBase *CreateEnumeratorCfgParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a parse handler for the statistics configuration
			static
			CParseHandlerBase *CreateStatisticsCfgParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a parse handler for the CTE configuration
			static
			CParseHandlerBase *CreateCTECfgParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a parse handler for the cost model configuration
			static
			CParseHandlerBase *CreateCostModelCfgParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct hint parse handler
			static
			CParseHandlerBase *CreateHintParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct window oids parse handler
			static
			CParseHandlerBase *CreateWindowOidsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a trace flag parse handler
			static 
			CParseHandlerBase *CreateTraceFlagsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a MD relation parse handler
			static 
			CParseHandlerBase *CreateMDRelationParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a MD external relation parse handler
			static
			CParseHandlerBase *CreateMDRelationExtParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a MD CTAS relation parse handler
			static
			CParseHandlerBase *CreateMDRelationCTASParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an MD index parse handler
			static 
			CParseHandlerBase *CreateMDIndexParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a relation stats parse handler
			static 
			CParseHandlerBase *CreateRelStatsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a column stats parse handler
			static 
			CParseHandlerBase *CreateColStatsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a column stats bucket parse handler
			static 
			CParseHandlerBase *CreateColStatsBucketParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an MD type parse handler
			static
			CParseHandlerBase *CreateMDTypeParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an MD scalarop parse handler
			static
			CParseHandlerBase *CreateMDScalarOpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an MD function parse handler
			static
			CParseHandlerBase *CreateMDFuncParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an MD aggregate operation parse handler
			static
			CParseHandlerBase *CreateMDAggParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an MD trigger parse handler
			static
			CParseHandlerBase *CreateMDTriggerParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an MD cast parse handler
			static
			CParseHandlerBase *CreateMDCastParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an MD scalar comparison parse handler
			static
			CParseHandlerBase *CreateMDScCmpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an MD check constraint parse handler
			static
			CParseHandlerBase *CreateMDChkConstraintParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a parse handler for a list of MD ids
			static
			CParseHandlerBase *CreateMDIdListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a metadata columns parse handler
			static
			CParseHandlerBase *CreateMDColsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			static
			CParseHandlerBase * CreateMDIndexInfoListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a column MD parse handler
			static
			CParseHandlerBase *CreateMDColParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a column default value expression parse handler
			static
			CParseHandlerBase *CreateColDefaultValExprParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar operator parse handler
			static
			CParseHandlerBase *CreateScalarOpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a properties parse handler
			static
			CParseHandlerBase *CreatePropertiesParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a filter operator parse handler
			static
			CParseHandlerBase *CreateFilterParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a table scan parse handler
			static
			CParseHandlerBase *CreateTableScanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a bitmap table scan parse handler
			static
			CParseHandlerBase *CreateBitmapTableScanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a dynamic bitmap table scan parse handler
			static
			CParseHandlerBase *CreateDynBitmapTableScanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an external scan parse handler
			static
			CParseHandlerBase *CreateExternalScanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a subquery scan parse handler
			static
			CParseHandlerBase *CreateSubqueryScanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a result node parse handler
			static
			CParseHandlerBase *CreateResultParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a HJ parse handler
			static
			CParseHandlerBase *CreateHashJoinParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a NLJ parse handler
			static
			CParseHandlerBase *CreateNLJoinParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a merge join parse handler
			static
			CParseHandlerBase *CreateMergeJoinParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a sort parse handler
			static
			CParseHandlerBase *CreateSortParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an append parse handler
			static
			CParseHandlerBase *CreateAppendParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a materialize parse handler
			static
			CParseHandlerBase *CreateMaterializeParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a dynamic table scan parse handler
			static
			CParseHandlerBase *CreateDTSParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a dynamic index scan parse handler
			static
			CParseHandlerBase *CreateDynamicIdxScanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a partition selector parse handler
			static
			CParseHandlerBase *CreatePartitionSelectorParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a sequence parse handler
			static
			CParseHandlerBase *CreateSequenceParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a limit (physical) parse handler
			static
			CParseHandlerBase *CreateLimitParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a limit count parse handler
			static
			CParseHandlerBase *CreateLimitCountParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a limit offset parse handler
			static
			CParseHandlerBase *CreateLimitOffsetParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a subquery parse handler
			static
			CParseHandlerBase *CreateScSubqueryParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a subquery parse handler
			static
			CParseHandlerBase *CreateScBitmapBoolOpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an array parse handler
			static
			CParseHandlerBase *CreateScArrayParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an arrayref parse handler
			static
			CParseHandlerBase *CreateScArrayRefParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an arrayref index list parse handler
			static
			CParseHandlerBase *CreateScArrayRefIdxListParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an assert predicate parse handler
			static
			CParseHandlerBase *CreateScAssertConstraintListParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);


			// construct a DML action parse handler
			static
			CParseHandlerBase *CreateScDMLActionParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a scalar operator list
			static
			CParseHandlerBase *CreateScOpListParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar part oid
			static
			CParseHandlerBase *CreateScPartOidParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar part default
			static
			CParseHandlerBase *CreateScPartDefaultParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar part bound
			static
			CParseHandlerBase *CreateScPartBoundParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar part bound inclusion
			static
			CParseHandlerBase *CreateScPartBoundInclParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar part bound openness
			static
			CParseHandlerBase *CreateScPartBoundOpenParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar part list values
			static
			CParseHandlerBase *CreateScPartListValuesParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar part list null test
			static
			CParseHandlerBase *CreateScPartListNullTestParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a direct dispatch info parse handler
			static
			CParseHandlerBase *CreateDirectDispatchParseHandler
				(
				IMemoryPool* mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a gather motion parse handler
			static
			CParseHandlerBase *CreateGatherMotionParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a broadcast motion parse handler
			static
			CParseHandlerBase *CreateBroadcastMotionParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a redistribute motion parse handler
			static
			CParseHandlerBase *CreateRedistributeMotionParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a routed motion parse handler
			static
			CParseHandlerBase *CreateRoutedMotionParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a random motion parse handler
			static
			CParseHandlerBase *CreateRandomMotionParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a physical aggregate parse handler
			static
			CParseHandlerBase *CreateAggParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an aggregate function parse handler
			static
			CParseHandlerBase *CreateAggRefParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a parse handler for a physical window node
			static
			CParseHandlerBase *CreateWindowParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an window function parse handler
			static
			CParseHandlerBase *CreateWindowRefParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an window frame parse handler
			static
			CParseHandlerBase *CreateWindowFrameParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an window key parse handler
			static
			CParseHandlerBase *CreateWindowKeyParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a parse handler to parse the list of window keys
			static
			CParseHandlerBase *CreateWindowKeyListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an window specification parse handler
			static
			CParseHandlerBase *CreateWindowSpecParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a parse handler to parse the list of window specifications
			static
			CParseHandlerBase *CreateWindowSpecListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a grouping column list parse handler
			static
			CParseHandlerBase *CreateGroupingColListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a comparison operator parse handler
			static
			CParseHandlerBase *CreateScCmpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a distinct compare parse handler
			static
			CParseHandlerBase *CreateDistinctCmpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a scalar identifier parse handler
			static
			CParseHandlerBase *CreateScIdParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a scalar operator parse handler
			static
			CParseHandlerBase *CreateScOpExprParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an array compare parse handler
			static
			CParseHandlerBase *CreateScArrayCmpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a boolean expression parse handler
			static
			CParseHandlerBase *CreateScBoolExprParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a min/max parse handler
			static
			CParseHandlerBase *CreateScMinMaxParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a boolean test parse handler
			static
			CParseHandlerBase *CreateBooleanTestParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a null test parse handler
			static
			CParseHandlerBase *CreateScNullTestParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a nullif parse handler
			static
			CParseHandlerBase *CreateScNullIfParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a cast parse handler
			static
			CParseHandlerBase *CreateScCastParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a coerce parse handler
			static
			CParseHandlerBase *CreateScCoerceToDomainParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a coerceviaio parse handler
			static
			CParseHandlerBase *CreateScCoerceViaIOParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a ArrayCoerceExpr parse handler
			static
			CParseHandlerBase *CreateScArrayCoerceExprParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a sub plan parse handler
			static
			CParseHandlerBase *CreateScSubPlanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// create a parse handler for parsing a SubPlan test expression
			static
			CParseHandlerBase *CreateScSubPlanTestExprParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a sub plan params parse handler
			static
			CParseHandlerBase *CreateScSubPlanParamListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a sub plan param parse handler
			static
			CParseHandlerBase *CreateScSubPlanParamParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical TVF parse handler
			static
			CParseHandlerBase *CreateLogicalTVFParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a physical TVF parse handler
			static
			CParseHandlerBase *CreatePhysicalTVFParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a coalesce parse handler
			static
			CParseHandlerBase *CreateScCoalesceParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a switch parse handler
			static
			CParseHandlerBase *CreateScSwitchParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a switch case parse handler
			static
			CParseHandlerBase *CreateScSwitchCaseParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a case test parse handler
			static
			CParseHandlerBase *CreateScCaseTestParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a constant parse handler
			static
			CParseHandlerBase *CreateScConstValueParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an if statement parse handler
			static
			CParseHandlerBase *CreateIfStmtParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a function parse handler
			static
			CParseHandlerBase *CreateScFuncExprParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a project list parse handler
			static
			CParseHandlerBase *CreateProjListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a project element parse handler
			static
			CParseHandlerBase *CreateProjElemParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a hash expression list parse handler
			static
			CParseHandlerBase *CreateHashExprListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);		
			
			// construct a hash expression parse handler
			static
			CParseHandlerBase *CreateHashExprParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a condition list parse handler
			static
			CParseHandlerBase *CreateCondListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a sort column list parse handler
			static
			CParseHandlerBase *CreateSortColListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a sort column parse handler
			static
			CParseHandlerBase *CreateSortColParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a cost parse handler
			static
			CParseHandlerBase *CreateCostParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a table descriptor parse handler
			static
			CParseHandlerBase *CreateTableDescParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a column descriptor parse handler
			static
			CParseHandlerBase *CreateColDescParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct an index scan list parse handler
			static
			CParseHandlerBase *CreateIdxScanListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an index only scan parse handler
			static
			CParseHandlerBase *CreateIdxOnlyScanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a bitmap index scan list parse handler
			static
			CParseHandlerBase *CreateBitmapIdxProbeParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an index descriptor list parse handler
			static
			CParseHandlerBase *CreateIdxDescrParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct an index condition list parse handler
			static
			CParseHandlerBase *CreateIdxCondListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);


			// construct a query parse handler
			static
			CParseHandlerBase *CreateQueryParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical get parse handler
			static
			CParseHandlerBase *CreateLogicalGetParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical external get parse handler
			static
			CParseHandlerBase *CreateLogicalExtGetParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical operator parse handler
			static
			CParseHandlerBase *CreateLogicalOpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical project parse handler
			static
			CParseHandlerBase *CreateLogicalProjParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical CTE producer parse handler
			static
			CParseHandlerBase *CreateLogicalCTEProdParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a logical CTE consumer parse handler
			static
			CParseHandlerBase *CreateLogicalCTEConsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a logical CTE anchor parse handler
			static
			CParseHandlerBase *CreateLogicalCTEAnchorParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a CTE list
			static
			CParseHandlerBase *CreateCTEListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a logical window parse handler
			static
			CParseHandlerBase *CreateLogicalWindowParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical insert parse handler
			static
			CParseHandlerBase *CreateLogicalInsertParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a logical delete parse handler
			static
			CParseHandlerBase *CreateLogicalDeleteParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical update parse handler
			static
			CParseHandlerBase *CreateLogicalUpdateParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a logical CTAS parse handler
			static
			CParseHandlerBase *CreateLogicalCTASParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a physical CTAS parse handler
			static
			CParseHandlerBase *CreatePhysicalCTASParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a parse handler for parsing CTAS storage options
			static
			CParseHandlerBase *CreateCTASOptionsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a physical CTE producer parse handler
			static
			CParseHandlerBase *CreatePhysicalCTEProdParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a physical CTE consumer parse handler
			static
			CParseHandlerBase *CreatePhysicalCTEConsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a physical DML parse handler
			static
			CParseHandlerBase *CreatePhysicalDMLParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a physical split parse handler
			static
			CParseHandlerBase *CreatePhysicalSplitParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a physical row trigger parse handler
			static
			CParseHandlerBase *CreatePhysicalRowTriggerParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a physical assert parse handler
			static
			CParseHandlerBase *CreatePhysicalAssertParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a logical set operator parse handler
			static
			CParseHandlerBase *CreateLogicalSetOpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical select parse handler
			static
			CParseHandlerBase *CreateLogicalSelectParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical join parse handler
			static
			CParseHandlerBase *CreateLogicalJoinParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical query output parse handler
			static
			CParseHandlerBase *CreateLogicalQueryOpParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a logical groupby parse handler
			static
			CParseHandlerBase *CreateLogicalGrpByParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);	

			// construct a logical limit parse handler
			static
			CParseHandlerBase *CreateLogicalLimitParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a logical const table parse handler
			static
			CParseHandlerBase *CreateLogicalConstTableParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a quantified subquery parse handler
			static
			CParseHandlerBase *CreateScScalarSubqueryQuantifiedParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
	
			// construct a subquery parse handler
			static
			CParseHandlerBase *CreateScScalarSubqueryExistsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a pass-through parse handler for stack traces
			static
			CParseHandlerBase *CreateStackTraceParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a statistics parse handler
			static
			CParseHandlerBase *CreateStatsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a derived column parse handler
			static
			CParseHandlerBase *CreateStatsDrvdColParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a derived relation stats parse handler
			static
			CParseHandlerBase *CreateStatsDrvdRelParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a bucket bound parse handler
			static
			CParseHandlerBase *CreateStatsBucketBoundParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);
			
			// construct a trailing window frame edge parser
			static
			CParseHandlerBase *CreateFrameTrailingEdgeParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a leading window frame edge parser
			static
			CParseHandlerBase *CreateFrameLeadingEdgeParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct search strategy parse handler
			static
			CParseHandlerBase *CreateSearchStrategyParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct search stage parse handler
			static
			CParseHandlerBase *CreateSearchStageParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct xform parse handler
			static
			CParseHandlerBase *CreateXformParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct cost params parse handler
			static
			CParseHandlerBase *CreateCostParamsParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct cost param parse handler
			static
			CParseHandlerBase *CreateCostParamParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar expression parse handler
			static
			CParseHandlerBase *CreateScExprParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a scalar values list parse handler
			static
			CParseHandlerBase *CreateScValuesListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a values scan parse handler
			static
			CParseHandlerBase *CreateValuesScanParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a md array coerce cast parse handler
			static
			CParseHandlerBase *CreateMDArrayCoerceCastParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// construct a nested loop param list parse handler
			static
			CParseHandlerBase *CreateNLJIndexParamListParseHandler
				(
				IMemoryPool *mp,
				CParseHandlerManager *parse_handler_manager,
				CParseHandlerBase *parse_handler_root
				);

			// construct a nested loop param parse handler
			static
			CParseHandlerBase *CreateNLJIndexParamParseHandler
				(
				IMemoryPool *pmp,
				CParseHandlerManager *parse_handler_manager,
				CParseHandlerBase *parse_handler_root
				);

		public:
			
			// initialize mappings of tokens to parse handlers
			static 
			void Init(IMemoryPool *mp);
			
			// return the parse handler creator for operator with the given name
			static 
			CParseHandlerBase *GetParseHandler
				(
				IMemoryPool *mp,
				const XMLCh *xml_str,
				CParseHandlerManager *parse_handler_mgr,
				CParseHandlerBase *parse_handler_root
				);

			// factory methods for creating parse handlers
			static 
			CParseHandlerDXL *GetParseHandlerDXL
				(
				IMemoryPool *mp,
				CParseHandlerManager*
				);
	};
}

#endif // !GPDXL_CParseHandlerFactory_H

// EOF
