//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CTranslatorDXLToPlStmt.h
//
//	@doc:
//		Class providing methods for translating from DXL tree to GPDB PlannedStmt
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CTranslatorDxlToPlStmt_H
#define GPDXL_CTranslatorDxlToPlStmt_H

#include "postgres.h"
#include "gpos/base.h"

#include "naucrates/dxl/operators/dxlops.h"
#include "naucrates/dxl/translate/CContextDXLToPlStmt.h"
#include "naucrates/dxl/translate/CDXLTranslateContext.h"
#include "naucrates/dxl/translate/CTranslatorDXLToScalar.h"
#include "naucrates/dxl/translate/CDXLTranslateContextBaseTable.h"
#include "naucrates/dxl/CIdGenerator.h"
#include "naucrates/dxl/translate/CMappingColIdVarPlStmt.h"

// fwd declarations
namespace gpopt
{
	class CMDAccessor;
}

struct PlannedStmt;
struct Scan;
struct HashJoin;
struct NestLoop;
struct MergeJoin;
struct Hash;
struct RangeTblEntry;
struct Motion;
struct Limit;
struct Agg;
struct Append;
struct Sort;
struct SubqueryScan;
struct SubPlan;
struct Result;
struct Material;
struct ShareInputScan;
typedef Scan SeqScan;
typedef OpExpr DistinctExpr;
//struct Const;
//struct List;

namespace gpdxl
{

	using namespace gpopt;

	class CDXLNode;

	//---------------------------------------------------------------------------
	//	@class:
	//		CTranslatorDXLToPlStmt
	//
	//	@doc:
	//		Class providing methods for translating from DXL tree to GPDB PlannedStmt
	//
	//---------------------------------------------------------------------------
	class CTranslatorDXLToPlStmt
	{
		// shorthand for functions for translating DXL operator nodes into planner trees
		typedef Plan * (CTranslatorDXLToPlStmt::*PfPplan)(const CDXLNode *pdxln, CDXLTranslateContext *pdxltrctxOut, Plan *pplanParent);

		private:
		
			// pair of DXL operator type and the corresponding translator
			struct STranslatorMapping
			{
				// type
				Edxlopid edxlopid;
				
				// translator function pointer
				PfPplan pf;
			};
				
			// memory pool
			IMemoryPool *m_pmp;
			
			// meta data accessor
			CMDAccessor *m_pmda;

			// DXL operator translators indexed by the operator id
			PfPplan m_rgpfTranslators[EdxlopSentinel];
			
			CContextDXLToPlStmt *m_pctxdxltoplstmt;

			CTranslatorDXLToScalar *m_pdxlsctranslator;
			
			// private copy ctor
			CTranslatorDXLToPlStmt(const CTranslatorDXLToPlStmt&);
			
		public:
			// ctor/dtor
			CTranslatorDXLToPlStmt(IMemoryPool *, CMDAccessor *, CContextDXLToPlStmt*);
			~CTranslatorDXLToPlStmt();
			
			// translate DXL operator node into a Plan node
			Plan *PplFromDXL
				(
				const CDXLNode *pdxln,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// main translation routine for DXL tree -> PlannedStmt
			PlannedStmt *PplstmtFromDXL(const CDXLNode *pdxln);

			// translate the join types from its DXL representation to the GPDB one
			static JoinType JtFromEdxljt(EdxlJoinType edxljt);

		private:
			
			// initialize index of operator translators
			void InitTranslators();

			// Set the bitmapset of a plan to the list of param_ids defined by the plan
			void SetParamIds(Plan *);

			// Set the qDispSliceId in the subplans defining an initplan
			void SetInitPlanSliceInformation(SubPlan *);

			// Set InitPlanVariable in PlannedStmt
			void SetInitPlanVariables(PlannedStmt *);

			// Returns the id of the plan;
			INT IPlanId(Plan* pplparent);
			
			// translate DXL table scan node into a SeqScan node
			Plan *PssFromDXLTblScan
				(
				const CDXLNode *pdxlnTblScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL index scan node into a IndexScan node
			Plan *PisFromDXLIndexScan
				(
				const CDXLNode *pdxlnIndexScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate DXL index only scan node into a IndexOnlyScan node
			Plan *PiosFromDXLIndexOnlyScan
				(
				const CDXLNode *pdxlnIndexOnlyScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translates a DXL index scan node into a IndexScan node
			Plan *PisFromDXLIndexScan
				(
				const CDXLNode *pdxlnIndexScan,
				CDXLPhysicalIndexScan *pdxlopIndexScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent,
				BOOL fIndexOnlyScan
				);

			// translate DXL hash join into a HashJoin node
			Plan *PhjFromDXLHJ
				(
				const CDXLNode *pdxlnHJ,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate DXL nested loop join into a NestLoop node
			Plan *PnljFromDXLNLJ
				(
				const CDXLNode *pdxlnNLJ,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate DXL merge join into a MergeJoin node
			Plan *PmjFromDXLMJ
				(
				const CDXLNode *pdxlnMJ,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate DXL motion node into GPDB Motion plan node
			Plan *PmotionFromDXLMotion
				(
				const CDXLNode *pdxlnMotion,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate DXL aggregate node into GPDB Agg plan node
			Plan *PaggFromDXLAgg
				(
				const CDXLNode *pdxlnMotion,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate DXL window node into GPDB window node
			Plan *PwindowFromDXLWindow
				(
				const CDXLNode *pdxlnMotion,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// translate the DXL window frame into GPDB window frame node
			WindowFrame *Pwindowframe
				(
				const CDXLWindowFrame *pdxlwf,
				const CDXLTranslateContext *pdxltrctxChild,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplan
				);

			// translate DXL sort node into GPDB Sort plan node
			Plan *PsortFromDXLSort
				(
				const CDXLNode *pdxlnSort,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate a DXL node into a Hash node
			Plan *PhhashFromDXL
				(
				const CDXLNode *pdxln,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate DXL Limit node into a Limit node
			Plan *PlimitFromDXLLimit
				(
				const CDXLNode *pdxlnLimit,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate DXL TVF into a GPDB Function Scan node
			Plan *PplanFunctionScanFromDXLTVF
				(
				const CDXLNode *pdxlnTVF,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			Plan *PsubqscanFromDXLSubqScan
				(
				const CDXLNode *pdxlnSubqScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			Plan *PresultFromDXLResult
				(
				const CDXLNode *pdxlnResult,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			Plan *PappendFromDXLAppend
				(
				const CDXLNode *pdxlnAppend,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			Plan *PmatFromDXLMaterialize
				(
				const CDXLNode *pdxlnMaterialize,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			Plan *PshscanFromDXLSharedScan
				(
				const CDXLNode *pdxlnSharedScan,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate a sequence operator
			Plan *PplanSequence
				(
				const CDXLNode *pdxlnSequence,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate a dynamic table scan operator
			Plan *PplanDTS
				(
				const CDXLNode *pdxlnDTS,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// create range table entry from a CDXLPhysicalTVF node
			RangeTblEntry *PrteFromDXLTVF
				(
				const CDXLNode *pdxlnTVF,
				CDXLTranslateContext *pdxltrctxOut,
				CDXLTranslateContextBaseTable *pdxltrctxbt,
				Plan *pplanParent
				);

			// create range table entry from a table descriptor
			RangeTblEntry *PrteFromTblDescr
				(
				const CDXLTableDescr *pdxltabdesc,
				const CDXLIndexDescr *pdxlid,
				Index iRel,
				CDXLTranslateContextBaseTable *pdxltrctxbtOut
				);
			
			// translate DXL projection list into a target list
			List *PlTargetListFromProjList
				(
				const CDXLNode *pdxlnPrL,
				const CDXLTranslateContextBaseTable *pdxltrctxbt,
				DrgPdxltrctx *pdrgpdxltrctx,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// create a target list containing column references for a hash node from the 
			// project list of its child node
			List *PlTargetListForHashNode
				(
				const CDXLNode *pdxlnPrL,
				CDXLTranslateContext *pdxltrctxChild,
				CDXLTranslateContext *pdxltrctxOut
				);
			
			List *PlQualFromFilter
				(
				const CDXLNode *pdxlnFilter,
				const CDXLTranslateContextBaseTable *pdxltrctxbt,
				DrgPdxltrctx *pdrgpdxltrctx,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);


			// translate operator costs from the DXL cost structure into the types
			// used by GPDB
			void TranslatePlanCosts
				(
				const CDXLOperatorCost *pdxlopcost,
				Cost *pcostStartupOut,
				Cost *pcostTotalOut,
				Cost *pcostRowsOut,
				INT *piWidthOut
				);
			
			// shortcut for translating both the projection list and the filter
			void TranslateProjListAndFilter
				(
				const CDXLNode *pdxlnPrL,
				const CDXLNode *pdxlnFilter,
				const CDXLTranslateContextBaseTable *pdxltrctxbt,
				DrgPdxltrctx *pdrgpdxltrctx,
				List **pplTargetListOut,
				List **pplQualOut,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			// translate the hash expr list of a redistribute motion node
			void TranslateHashExprList
				(
				const CDXLNode *pdxlnHashExprList,
				const CDXLTranslateContext *pdxltrctxChild,
				List **pplHashExprOut,
				List **pplHashExprTypesOut,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);
			
			void TranslateSortCols
				(
				const CDXLNode *pdxlnSortColList,
				const CDXLTranslateContext *pdxltrctxChild,
				AttrNumber *pattnoSortColIds,
				Oid *poidSortOpIds
				);
					
			List *PlQualFromScalarCondNode
				(
				const CDXLNode *pdxlnFilter,
				const CDXLTranslateContextBaseTable *pdxltrctxbt,
				DrgPdxltrctx *pdrgpdxltrctx,
				CDXLTranslateContext *pdxltrctxOut,
				Plan *pplanParent
				);

			// parse string value into a Const
			static 
			Cost CostFromStr(const CWStringBase *pstr);
			
			// check if the given operator is a DML operator on a distributed table
			BOOL FTargetTableDistributed(CDXLOperator *pdxlop);

	};
}

#endif // !GPDXL_CTranslatorDxlToPlStmt_H

// EOF
