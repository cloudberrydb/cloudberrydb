//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CMDFunctionGPDB.h
//
//	@doc:
//		Implementation of GPDB-specific functions in the metadata cache
//---------------------------------------------------------------------------


#ifndef GPMD_CMDFunctionGPDB_H
#define GPMD_CMDFunctionGPDB_H

#include "gpos/base.h"

#include "naucrates/md/IMDFunction.h"
#include "naucrates/dxl/xml/CXMLSerializer.h"

namespace gpmd
{
	
	using namespace gpdxl;

	//---------------------------------------------------------------------------
	//	@class:
	//		CMDFunctionGPDB
	//
	//	@doc:
	//		Implementation for GPDB-specific functions in the metadata cache
	//
	//---------------------------------------------------------------------------
	class CMDFunctionGPDB : public IMDFunction
	{
		private:
			// memory pool
			IMemoryPool *m_pmp;
			
			// DXL for object
			const CWStringDynamic *m_pstr;
			
			// func id
			IMDId *m_pmdid;
			
			// func name
			CMDName *m_pmdname;
			
			// result type
			IMDId *m_pmdidTypeResult;
			
			// output argument types
			DrgPmdid *m_pdrgpmdidTypes;

			// whether function returns a set of values
			BOOL m_fReturnsSet;
			
			// function stability
			EFuncStbl m_efsStability;
			
			// function data access
			EFuncDataAcc m_efdaDataAccess;

			// function strictness (i.e. whether func returns NULL on NULL input)
			BOOL m_fStrict;

			// dxl token array for stability
			Edxltoken m_drgDxlStability[EfsSentinel];

			// dxl token array for data access
			Edxltoken m_drgDxlDataAccess[EfdaSentinel];
			
			// returns the string representation of the function stability
			const CWStringConst *PstrStability() const;

			// returns the string representation of the function data access
			const CWStringConst *PstrDataAccess() const;

			// serialize the array of output arg types into a comma-separated string
			CWStringDynamic *PstrOutArgTypes() const;

			// initialize dxl token arrays
			void InitDXLTokenArrays();

			// private copy ctor
			CMDFunctionGPDB(const CMDFunctionGPDB &);
			
		public:
			// ctor/dtor
			CMDFunctionGPDB
				(
				IMemoryPool *pmp,
				IMDId *pmdid,
				CMDName *pmdname,
				IMDId *pmdidTypeResult,
				DrgPmdid *pdrgpmdidTypes,
				BOOL fReturnsSet,
				EFuncStbl efsStability,
				EFuncDataAcc efdaDataAccess,
				BOOL fStrict
				);
			
			virtual
			~CMDFunctionGPDB();
			
			// accessors
			virtual 
			const CWStringDynamic *Pstr() const
			{
				return m_pstr;
			}
			
			// function id
			virtual 
			IMDId *Pmdid() const;
			
			// function name
			virtual 
			CMDName Mdname() const;
			
			// result type
			virtual 
			IMDId *PmdidTypeResult() const;

			// output argument types
			virtual
			DrgPmdid *PdrgpmdidOutputArgTypes() const;

			// does function return NULL on NULL input
			virtual 
			BOOL FStrict() const
			{
				return m_fStrict;
			}
			
			// function stability
			virtual
			EFuncStbl EfsStability() const
			{
				return m_efsStability;
			}

			// function data access
			virtual
			EFuncDataAcc EfdaDataAccess() const
			{
				return m_efdaDataAccess;
			}

			// does function return a set of values
			virtual 
			BOOL FReturnsSet() const;
			
			// serialize object in DXL format
			virtual 
			void Serialize(gpdxl::CXMLSerializer *pxmlser) const;
			
#ifdef GPOS_DEBUG
			// debug print of the type in the provided stream
			virtual 
			void DebugPrint(IOstream &os) const;
#endif
	};
}

#endif // !GPMD_CMDFunctionGPDB_H

// EOF
