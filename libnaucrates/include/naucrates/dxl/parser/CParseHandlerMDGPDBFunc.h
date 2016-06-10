//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDGPDBFunc.h
//
//	@doc:
//		SAX parse handler class for GPDB function metadata
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDGPDBFunc_H
#define GPDXL_CParseHandlerMDGPDBFunc_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"


#include "naucrates/md/CMDFunctionGPDB.h"


namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDGPDBFunc
	//
	//	@doc:
	//		Parse handler for GPDB function metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDGPDBFunc : public CParseHandlerMetadataObject
	{
		private:
			// id and version 
			IMDId *m_pmdid;
			
			// name
			CMDName *m_pmdname;
					
			// result type
			IMDId *m_pmdidTypeResult;
			
			// output argument types
			DrgPmdid *m_pdrgpmdidTypes;

			// whether function returns a set of values
			BOOL m_fReturnsSet;
			
			// function stability
			CMDFunctionGPDB::EFuncStbl m_efuncstbl;
			
			// function data access
			CMDFunctionGPDB::EFuncDataAcc m_efuncdataacc;

			// function strictness (i.e. whether func returns NULL on NULL input)
			BOOL m_fStrict;
			
			// private copy ctor
			CParseHandlerMDGPDBFunc(const CParseHandlerMDGPDBFunc &);
			
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
			
			// parse function stability property from XML string
			CMDFunctionGPDB::EFuncStbl EFuncStability(const XMLCh *xmlsz);

			// parse function data access property from XML string
			CMDFunctionGPDB::EFuncDataAcc EFuncDataAccess(const XMLCh *xmlsz);

		public:
			// ctor
			CParseHandlerMDGPDBFunc
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);			
	};
}

#endif // !GPDXL_CParseHandlerMDGPDBFunc_H

// EOF
