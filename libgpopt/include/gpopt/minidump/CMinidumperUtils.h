//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2012 EMC Corp.
//
//	@filename:
//		CMinidumperUtils.h
//
//	@doc:
//		Minidump utility functions
//---------------------------------------------------------------------------
#ifndef GPOPT_CMiniDumperUtils_H
#define GPOPT_CMiniDumperUtils_H

#include "gpos/base.h"
#include "gpos/error/CMiniDumper.h"

#include "gpopt/minidump/CDXLMinidump.h"

using namespace gpos;

namespace gpopt
{

	// fwd decl
	class ICostModel;
	class CMiniDumperDXL;
	class COptimizerConfig;
	class IConstExprEvaluator;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CMinidumperUtils
	//
	//	@doc:
	//		Minidump utility functions
	//
	//---------------------------------------------------------------------------
	class CMinidumperUtils
	{
		public:
			// dump to file
			static
			void Dump(const CHAR *szFileName, CMiniDumperDXL *pmdmp);
		
			// load a minidump
			static
			CDXLMinidump *PdxlmdLoad(IMemoryPool *pmp, const CHAR *szFileName);
			
			// generate a minidump file name in the provided buffer
			static
			void GenerateMinidumpFileName(CHAR *szBuf, ULONG ulLength, ULONG ulSessionId, ULONG ulCmdId, const CHAR *szMinidumpFileName = NULL);
			
			// finalize minidump and dump to a file
			static 
			void Finalize
				(
				CMiniDumperDXL *pmdp,
				BOOL fSerializeErrCtx,
				ULONG ulSessionId,
				ULONG ulCmdId,
				const CHAR *szMinidumpFileName = NULL	// name of minidump file to be created
				);
			
			// load and execute the minidump in the specified file
			static
			CDXLNode *PdxlnExecuteMinidump
				(
				IMemoryPool *pmp, 
				const CHAR *szFileName,
				ULONG ulSegments, 
				ULONG ulSessionId, 
				ULONG ulCmdId,
				COptimizerConfig *poconf,
				IConstExprEvaluator *pceeval = NULL
				);
			
			// execute the given minidump
			static
			CDXLNode *PdxlnExecuteMinidump
				(
				IMemoryPool *pmp, 
				CDXLMinidump *pdxlmdp,
				const CHAR *szFileName, 
				ULONG ulSegments, 
				ULONG ulSessionId, 
				ULONG ulCmdId,
				COptimizerConfig *poconf,
				IConstExprEvaluator *pceeval = NULL
				);
			
			// execute the given minidump using the given MD accessor
			static
			CDXLNode *PdxlnExecuteMinidump
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CDXLMinidump *pdxlmd,
				const CHAR *szFileName,
				ULONG ulSegments,
				ULONG ulSessionId,
				ULONG ulCmdId,
				COptimizerConfig *poconf,
				IConstExprEvaluator *pceeval
				);

	}; // class CMinidumperUtils

}

#endif // !GPOPT_CMiniDumperUtils_H

// EOF

