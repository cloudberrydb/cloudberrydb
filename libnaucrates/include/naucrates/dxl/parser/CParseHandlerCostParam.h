//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2013 EMC Corp.
//
//	@filename:
//		CParseHandlerCostParam.h
//
//	@doc:
//		SAX parse handler class for parsing cost model param.
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerCostParam_H
#define GPDXL_CParseHandlerCostParam_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"

namespace gpdxl
{

	using namespace gpos;

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerCostParam
	//
	//	@doc:
	//		Parse handler for parsing cost model param
	//
	//---------------------------------------------------------------------------
	class CParseHandlerCostParam : public CParseHandlerBase
	{

		private:

			// param name
			CHAR *m_szName;

			// param value
			CDouble m_dVal;

			// lower bound value
			CDouble m_dLowerBound;

			// upper bound value
			CDouble m_dUpperBound;

			// private copy ctor
			CParseHandlerCostParam(const CParseHandlerCostParam&);

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

			// ctor
			CParseHandlerCostParam
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			// dtor
			virtual
			~CParseHandlerCostParam();

			// return parsed param name
			CHAR *SzName() const
			{
				return m_szName;
			}

			// return parsed param value
			CDouble DVal() const
			{
				return m_dVal;
			}

			// return parsed param lower bound value
			CDouble DLowerBound() const
			{
				return m_dLowerBound;
			}

			// return parsed param upper bound value
			CDouble DUpperBound() const
			{
				return m_dUpperBound;
			}

			EDxlParseHandlerType Edxlphtype() const
			{
				return EdxlphCostParam;
			}

	};
}

#endif // !GPDXL_CParseHandlerCostParam_H

// EOF
