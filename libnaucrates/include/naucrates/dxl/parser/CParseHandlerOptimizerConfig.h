//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerOptimizerConfig.h
//
//	@doc:
//		SAX parse handler class for parsing optimizer config options
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerOptimizerConfig_H
#define GPDXL_CParseHandlerOptimizerConfig_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

// fwd decl
namespace gpos
{
	class CBitSet;
}

namespace gpdxl
{
	using namespace gpos;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerOptimizerConfig
	//
	//	@doc:
	//		SAX parse handler class for parsing optimizer config options
	//
	//---------------------------------------------------------------------------
	class CParseHandlerOptimizerConfig : public CParseHandlerBase
	{
		private:
			
			// trace flag bitset
			CBitSet *m_pbs;
		
			// optimizer configuration
			COptimizerConfig *m_poconf;
			
			// private copy ctor
			CParseHandlerOptimizerConfig(const CParseHandlerOptimizerConfig&); 
		
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
			
		public:
			// ctor/dtor
			CParseHandlerOptimizerConfig
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			virtual ~CParseHandlerOptimizerConfig();
			
			// type of the parse handler
			EDxlParseHandlerType Edxlphtype() const;
			
			// trace flags
			CBitSet *Pbs() const;
			
			// optimizer config
			COptimizerConfig *Poconf() const;
	};
}

#endif // !GPDXL_CParseHandlerOptimizerConfig_H

// EOF
