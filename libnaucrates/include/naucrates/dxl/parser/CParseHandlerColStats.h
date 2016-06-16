//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerColStats.h
//
//	@doc:
//		SAX parse handler class for parsing base relation column stats objects
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerColStats_H
#define GPDXL_CParseHandlerColStats_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"

// fwd decl
namespace gpmd
{
	class CMDIdColStats;
}

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerColStats
	//
	//	@doc:
	//		Parse handler class for base relation column stats
	//
	//---------------------------------------------------------------------------
	class CParseHandlerColStats : public CParseHandlerMetadataObject
	{
		private:
		
			// mdid of the col stats object
			CMDIdColStats *m_pmdidColStats;
			
			// name of the column
			CMDName *m_pmdname;
			
			// column width
			CDouble m_dWidth;
			
			// null fraction
			CDouble m_dNullFreq;

			// ndistinct of remaining tuples
			CDouble m_dDistinctRemain;

			// frequency of remaining tuples
			CDouble m_dFreqRemain;

			// is the column statistics missing in the database
			BOOL m_fColStatsMissing;

			// private copy ctor
			CParseHandlerColStats(const CParseHandlerColStats&);

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
			CParseHandlerColStats
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
	};
}

#endif // !GPDXL_CParseHandlerColStats_H

// EOF
