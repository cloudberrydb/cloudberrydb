//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 Greenplum, Inc.
//
//	@filename:
//		CParseHandlerMDRelation.h
//
//	@doc:
//		SAX parse handler class for relation metadata
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerMDRelation_H
#define GPDXL_CParseHandlerMDRelation_H

#include "gpos/base.h"
#include "naucrates/dxl/parser/CParseHandlerMetadataObject.h"
#include "naucrates/dxl/xml/dxltokens.h"

#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/CMDPartConstraintGPDB.h"

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;

	XERCES_CPP_NAMESPACE_USE
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CParseHandlerMDRelation
	//
	//	@doc:
	//		Parse handler for relation metadata
	//
	//---------------------------------------------------------------------------
	class CParseHandlerMDRelation : public CParseHandlerMetadataObject
	{
		protected:
			// id and version of the relation
			IMDId *m_pmdid;
			
			// schema name
			CMDName *m_pmdnameSchema;
			
			// table name
			CMDName *m_pmdname;
			
			// is this a temporary relation
			BOOL m_fTemporary;
			
			// does this relation have oids
			BOOL m_fHasOids;

			// storage type
			IMDRelation::Erelstoragetype m_erelstorage;
			
			// distribution policy
			IMDRelation::Ereldistrpolicy m_ereldistrpolicy;
			
			// distribution columns
			DrgPul *m_pdrgpulDistrColumns;
			
			// do we need to consider a hash distributed table as random distributed
			BOOL m_fConvertHashToRandom;
			
			// partition keys
			DrgPul *m_pdrgpulPartColumns;

			// number of partitions
			ULONG m_ulPartitions;
			
			// key sets
			DrgPdrgPul *m_pdrgpdrgpulKeys;

			// part constraint
			CMDPartConstraintGPDB *m_ppartcnstr;
			
			// levels that include default partitions
			DrgPul *m_pdrgpulDefaultParts;

			// is part constraint unbounded
			BOOL m_fPartConstraintUnbounded; 
			
			// helper function to parse main relation attributes: name, id,
			// distribution policy and keys
			void ParseRelationAttributes(const Attributes& attrs, Edxltoken edxltokenElement);

			// create and activate the parse handler for the children nodes
			void ParseChildNodes();

		private:
			// private copy ctor
			CParseHandlerMDRelation(const CParseHandlerMDRelation &);
			
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
			CParseHandlerMDRelation
				(
				IMemoryPool *pmp,
				CParseHandlerManager *pphm,
				CParseHandlerBase *pphRoot
				);			
	};
}

#endif // !GPDXL_CParseHandlerMDRelation_H

// EOF
