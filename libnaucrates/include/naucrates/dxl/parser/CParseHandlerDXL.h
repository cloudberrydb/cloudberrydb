//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerDXL.h
//
//	@doc:
//		SAX parse handler class for parsing a DXL document
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerDXL_H
#define GPDXL_CParseHandlerDXL_H

#include "gpos/base.h"
#include "gpos/common/CBitSet.h"

#include "gpopt/cost/ICostModelParams.h"
#include "gpopt/search/CSearchStage.h"

#include "naucrates/dxl/parser/CParseHandlerBase.h"

#include "naucrates/md/CDXLStatsDerivedRelation.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	// shorthand for functions for translating GPDB expressions into DXL nodes
	typedef void (CParseHandlerDXL::*Pfparse)(CParseHandlerBase *pph);

	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerDXL
	//
	//	@doc:
	//		Parse handler for DXL documents.
	//		Starting point for all other parse handlers
	//
	//---------------------------------------------------------------------------
	class CParseHandlerDXL : public CParseHandlerBase
	{
		private:

			// pair of parse handler type and parse handler function
			struct SParseElem
			{
				EDxlParseHandlerType edxlphtype; // parse handler type
				Pfparse pf; // pointer to corresponding function
			};

			// traceflags
			CBitSet *m_pbs;
			
			// optimizer config
			COptimizerConfig *m_poconf;
			
			// MD request
			CMDRequest *m_pmdr;
			
			// the root of the parsed DXL query
			CDXLNode *m_pdxlnQuery;
			
			// list of query output columns
			DrgPdxln *m_pdrgpdxlnOutputCols;

			// list of CTE producers
			DrgPdxln *m_pdrgpdxlnCTE;

			// the root of the parsed DXL plan
			CDXLNode *m_pdxlnPlan;

			// list of parsed metadata objects
			DrgPimdobj *m_pdrgpmdobj;
			
			// list of parsed metadata ids
			DrgPmdid *m_pdrgpmdid;

			// the root of the parsed scalar expression
			CDXLNode *m_pdxlnScalarExpr;

			// list of source system ids
			DrgPsysid *m_pdrgpsysid;
			
			// list of parsed statistics objects
			DrgPdxlstatsderrel *m_pdrgpdxlstatsderrel;

			// search strategy
			DrgPss *m_pdrgpss;

			// plan Id
			ULLONG m_ullPlanId;

			// plan space size
			ULLONG m_ullPlanSpaceSize;

			// cost model params
			ICostModelParams *m_pcp;

			// private copy ctor
			CParseHandlerDXL(const CParseHandlerDXL&);
			
			// process the start of an element
			void StartElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
				);
				
			// process the end of an element
			void EndElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
				);
			
			// find the parse handler function for the given type
			Pfparse FindParseHandler(EDxlParseHandlerType edxlphtype);

			// extract traceflags
			void ExtractTraceFlags(CParseHandlerBase *pph);
			
			// extract optimizer config
			void ExtractOptimizerConfig(CParseHandlerBase *pph);

			// extract a physical plan
			void ExtractDXLPlan(CParseHandlerBase *pph);

			// extract metadata objects
			void ExtractMetadataObjects(CParseHandlerBase *pph);

			// extract statistics
			void ExtractStats(CParseHandlerBase *pph);

			// extract DXL query
			void ExtractDXLQuery(CParseHandlerBase *pph);

			// extract mdids of requested objects
			void ExtractMDRequest(CParseHandlerBase *pph);
			
			// extract search strategy
			void ExtractSearchStrategy(CParseHandlerBase *pph);

			// extract cost params
			void ExtractCostParams(CParseHandlerBase *pph);

            // extract a top level scalar expression
			void ExtractScalarExpr(CParseHandlerBase *pph);

			// check if given element name is valid for starting DXL document
			static
			BOOL FValidStartElement(const XMLCh* const xmlszName);

		public:
			// ctor
			CParseHandlerDXL(IMemoryPool *pmp, CParseHandlerManager *pphm);
			
			//dtor
			virtual
			~CParseHandlerDXL();
			
			// traceflag bitset
			CBitSet *Pbs() const;
			
			// optimizer config
			COptimizerConfig *Poconf() const;
			
			// returns the root of the parsed DXL query
			CDXLNode *PdxlnQuery() const;
			
			// returns the list of query output columns
			DrgPdxln *PdrgpdxlnOutputCols() const;

			// returns the list of CTE producers
			DrgPdxln *PdrgpdxlnCTE() const;

			// returns the root of the parsed DXL plan
			CDXLNode *PdxlnPlan() const;

			// return the list of parsed metadata objects
			DrgPimdobj *Pdrgpmdobj() const;

			// return the list of parsed metadata ids
			DrgPmdid *Pdrgpmdid() const;

			// return the MD request object
			CMDRequest *Pmdr() const;

			// return the root of the parsed scalar expression
			CDXLNode *PdxlnScalarExpr() const;

			// return the list of parsed source system id objects
			DrgPsysid *Pdrgpsysid() const;
			
			// return the list of statistics objects
			DrgPdxlstatsderrel *Pdrgpdxlstatsderrel() const;

			// return search strategy
			DrgPss *Pdrgpss() const;

			// return plan id
			ULLONG UllPlanId() const;

			// return plan space size
			ULLONG UllPlanSpaceSize() const;

			// return cost params
			ICostModelParams *Pcp() const;

			// process the end of the document
			void endDocument();
	};
}

#endif // !GPDXL_CParseHandlerDXL_H

// EOF
