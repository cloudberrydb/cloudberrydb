//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 Pivotal Inc.
//
//	@filename:
//		CDXLCtasStorageOptions.h
//
//	@doc:
//		Class for CTAS storage options
//
//	@owner: 
//		
//
//	@test:
//
//
//---------------------------------------------------------------------------



#ifndef GPDXL_CDXLCTASStorageOptions_H
#define GPDXL_CDXLCTASStorageOptions_H

#include "gpos/base.h"
#include "gpos/common/CDynamicPtrArray.h"
#include "gpos/common/CRefCount.h"
#include "naucrates/md/CMDName.h"

using namespace gpmd;

namespace gpdxl
{

	// fwd decl 
	class CXMLSerializer;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLCtasStorageOptions
	//
	//	@doc:
	//		Class for representing CTAS storage options
	//
	//---------------------------------------------------------------------------
	class CDXLCtasStorageOptions : public CRefCount
	{
		public:
			struct CDXLCtasOption
			{
				// the type of the Option encoded as an integer
				ULONG m_ulType;

				// option name
				CWStringBase *m_pstrName;
				
				// option value
				CWStringBase *m_pstrValue;
				
				// does this represent a NULL value
				BOOL m_fNull;

				// ctor
				CDXLCtasOption
					(
					ULONG ulType,
					CWStringBase *pstrName,
					CWStringBase *pstrValue,
					BOOL fNull
					)
					:
					m_ulType(ulType),
					m_pstrName(pstrName),
					m_pstrValue(pstrValue),
					m_fNull(fNull)
				{
					GPOS_ASSERT(NULL != pstrName);
					GPOS_ASSERT(NULL != pstrValue);
				}
				
				// dtor
				~CDXLCtasOption()
				{
					GPOS_DELETE(m_pstrName);
					GPOS_DELETE(m_pstrValue);
				}
				
			};
			
			typedef CDynamicPtrArray<CDXLCtasOption, CleanupDelete> DrgPctasOpt;
			
			//-------------------------------------------------------------------
			//	@enum:
			//		ECtasOnCommitAction
			//
			//	@doc:
			//		On commit specification for temporary tables created with CTAS
			//		at the end of a transaction block
			//
			//-------------------------------------------------------------------
			enum ECtasOnCommitAction
			{
				EctascommitNOOP,		// no action
				EctascommitPreserve,	// rows are preserved
				EctascommitDelete,		// rows are deleted
				EctascommitDrop,		// table is dropped
				EctascommitSentinel
			};
			
		private:

			// tablespace name
			CMDName *m_pmdnameTablespace;
			
			// on commit action spec
			ECtasOnCommitAction m_ectascommit;
			
			// array of name-value pairs of storage options
			DrgPctasOpt *m_pdrgpctasopt;
			
			// private copy ctor
			CDXLCtasStorageOptions(const CDXLCtasStorageOptions &);
		
			// string representation of OnCommit action
			static
			const CWStringConst *PstrOnCommitAction(ECtasOnCommitAction ectascommit);
			
		public:
			// ctor
			CDXLCtasStorageOptions(CMDName *pmdnameTablespace, ECtasOnCommitAction ectascommit, DrgPctasOpt *pdrgpctasopt);
			
			// dtor
			virtual
			~CDXLCtasStorageOptions();
			
			// accessor to tablespace name
			CMDName *PmdnameTablespace() const;
			
			// on commit action
			CDXLCtasStorageOptions::ECtasOnCommitAction Ectascommit() const;
			
			// accessor to options
			DrgPctasOpt *Pdrgpctasopt() const;
			
			// serialize to DXL
			void Serialize(CXMLSerializer *pxmlser) const;
	};
}

#endif // !GPDXL_CDXLCTASStorageOptions_H

// EOF
