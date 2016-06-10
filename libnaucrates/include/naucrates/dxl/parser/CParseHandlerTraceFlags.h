//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerTraceFlags.h
//
//	@doc:
//		SAX parse handler class for parsing a set of traceflags
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerTraceFlags_H
#define GPDXL_CParseHandlerTraceFlags_H

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
	//		CParseHandlerTraceFlags
	//
	//	@doc:
	//		SAX parse handler class for parsing the list of output segment indices in a 
	//		redistribute motion node.
	//
	//---------------------------------------------------------------------------
	class CParseHandlerTraceFlags : public CParseHandlerBase
	{
		private:
			
			// trace flag bitset
			CBitSet *m_pbs;
		
			// private copy ctor
			CParseHandlerTraceFlags(const CParseHandlerTraceFlags&); 
		
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
			CParseHandlerTraceFlags
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			virtual ~CParseHandlerTraceFlags();
			
			// type of the parse handler
			EDxlParseHandlerType Edxlphtype() const;
			
			// accessor
			CBitSet *Pbs();
	};
}

#endif // !GPDXL_CParseHandlerTraceFlags_H

// EOF
