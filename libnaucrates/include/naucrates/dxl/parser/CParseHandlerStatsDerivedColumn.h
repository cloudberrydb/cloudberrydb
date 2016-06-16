//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CParseHandlerStatsDerivedColumn.h
//
//	@doc:
//		Parse handler for derived column statistics
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerStatsDerivedColumn_H
#define GPDXL_CParseHandlerStatsDerivedColumn_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerBase.h"
#include "naucrates/md/CDXLStatsDerivedColumn.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE

	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerStatsDerivedColumn
	//
	//	@doc:
	//		Parse handler for derived column statistics
	//
	//---------------------------------------------------------------------------
	class CParseHandlerStatsDerivedColumn : public CParseHandlerBase
	{
		private:

			// column id
			ULONG m_ulColId;

			// width
			CDouble m_dWidth;

			// null fraction
			CDouble m_dNullFreq;

			// ndistinct of remaining tuples
			CDouble m_dDistinctRemain;

			// frequency of remaining tuples
			CDouble m_dFreqRemain;

			// derived column stats
			CDXLStatsDerivedColumn *m_pstatsdercol;

			// private copy ctor
			CParseHandlerStatsDerivedColumn(const CParseHandlerStatsDerivedColumn &);

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
			CParseHandlerStatsDerivedColumn
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);

			//dtor
			~CParseHandlerStatsDerivedColumn();

			// derived column stats
			CDXLStatsDerivedColumn *Pstatsdercol() const
			{
				return m_pstatsdercol;
			}
	};
}

#endif // !GPDXL_CParseHandlerStatsDerivedColumn_H

// EOF
