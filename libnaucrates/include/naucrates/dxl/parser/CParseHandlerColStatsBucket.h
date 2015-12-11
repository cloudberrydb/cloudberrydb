//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerColStatsBucket.h
//
//	@doc:
//		SAX parse handler class for parsing a bucket of a column stats object
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerColStatsBucket_H
#define GPDXL_CParseHandlerColStatsBucket_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"

// fwd decl
namespace gpmd
{
	class CMDIdColStats;
	class CDXLBucket;
}

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	// fwd decl
	class CDXLDatum;

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerColStatsBucket
	//
	//	@doc:
	//		Parse handler class for buckets of column stats objects
	//
	//---------------------------------------------------------------------------
	class CParseHandlerColStatsBucket : public CParseHandlerBase
	{
		private:
		
			// frequency
			CDouble m_dFrequency;
			
			// distinct values
			CDouble m_dDistinct;

			// lower bound value for the bucket
			CDXLDatum *m_pdxldatumLower;
			
			// upper bound value for the bucket
			CDXLDatum *m_pdxldatumUpper;
			
			// is lower bound closed
			BOOL m_fLowerClosed;

			// is upper bound closed
			BOOL m_fUpperClosed;

			// dxl bucket object
			CDXLBucket *m_pdxlbucket;
			
			// private copy ctor
			CParseHandlerColStatsBucket(const CParseHandlerColStatsBucket&);

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
			CParseHandlerColStatsBucket
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);
			
			// dtor
			virtual
			~CParseHandlerColStatsBucket();
			
			// returns the constructed bucket
			CDXLBucket *Pdxlbucket() const;
	};
}

#endif // !GPDXL_CParseHandlerColStatsBucket_H

// EOF
