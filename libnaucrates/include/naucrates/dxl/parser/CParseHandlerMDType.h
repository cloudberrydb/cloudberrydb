//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CParseHandlerMDType.h
//
//	@doc:
//		SAX parse handler class for GPDB types metadata
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDGPDBType_H
#define GPDXL_CParseHandlerMDGPDBType_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"
#include "naucrates/dxl/xml/dxltokens.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDType
	//
	//	@doc:
	//		Parse handler for GPDB types metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDType : public CParseHandlerMetadataObject
	{
		private:
			
			// structure mapping DXL mdid names to their corresponding member variable
			struct SMdidMapElem
			{
				// mdid name token
				Edxltoken m_edxltoken;
				
				// address of the member variable for that name
				IMDId **m_ppmdid;
			};
			
			// id and version of the type
			IMDId *m_pmdid;
			
			// type name
			CMDName *m_pmdname;
			
			// is this a fixed-length type
			BOOL m_fFixedLength;
			
			// type length
			INT m_iLength;
			
			// is type redistributable
			BOOL m_fRedistributable;
			
			// is type passed by value or by reference
			BOOL m_fByValue;
			
			// id of equality operator for type
			IMDId *m_pmdidOpEq;
			
			// id of inequality operator for type
			IMDId *m_pmdidOpNEq;

			// id of less than operator for type
			IMDId *m_pmdidOpLT;
			
			// id of less than equals operator for type
			IMDId *m_pmdidOpLEq;

			// id of greater than operator for type
			IMDId *m_pmdidOpGT;
			
			// id of greater than equals operator for type
			IMDId *m_pmdidOpGEq;

			// id of comparison operator for type used in btree lookups
			IMDId *m_pmdidOpComp;
			
			// id of min aggregate
			IMDId *m_pmdidMin;
						
			// id of max aggregate
			IMDId *m_pmdidMax;

			// id of avg aggregate
			IMDId *m_pmdidAvg;

			// id of sum aggregate
			IMDId *m_pmdidSum;

			// id of count aggregate
			IMDId *m_pmdidCount;

			// is type hashable
			BOOL m_fHashable;			

			// is type composite
			BOOL m_fComposite;

			// id of the relation corresponding to a composite type
			IMDId *m_pmdidBaseRelation;

			// id of array type
			IMDId *m_pmdidTypeArray;
			
			// private copy ctor
			CParseHandlerMDType(const CParseHandlerMDType &);
			
			// retrieves the address MDId member variable corresponding to the specified token
			IMDId **Ppmdid(Edxltoken edxltoken);

			// handles a SAX start element event
			void StartElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
 					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname,		// element's qname
					const Attributes& attr				// element's attributes
				);
			
			// handles a SAX endelement event
			void EndElement
				(
					const XMLCh* const xmlszUri, 		// URI of element's namespace
					const XMLCh* const xmlszLocalname,	// local part of element's name
					const XMLCh* const xmlszQname		// element's qname
				);
			
			// parse the value for the given mdid variable name from the attributes 
			void ParseMdid(const XMLCh *xmlszLocalname, const Attributes& attrs);
						
			BOOL FBuiltInType(const IMDId *pmdid) const;
			
		public:
			// ctor
			CParseHandlerMDType
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);		
			
			// dtor
			virtual
			~CParseHandlerMDType();
	};
}

#endif // !GPDXL_CParseHandlerMDGPDBType_H

// EOF
