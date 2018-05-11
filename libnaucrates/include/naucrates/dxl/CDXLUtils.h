//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLUtils.h
//
//	@doc:
//		Entry point for parsing and serializing DXL documents.
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLUtils_H
#define GPDXL_CDXLUtils_H

#include "gpos/base.h"
#include "gpos/common/CAutoP.h"
#include "gpos/io/IOstream.h"
#include "gpos/string/CWStringDynamic.h"

#include "naucrates/dxl/operators/CDXLNode.h"
#include "naucrates/dxl/xml/dxltokens.h"

#include "naucrates/md/CMDIdGPDB.h"
#include "naucrates/md/IMDCacheObject.h"

#include "naucrates/statistics/CStatistics.h"

#include <xercesc/util/XMLString.hpp>

namespace gpmd
{
	class CMDRequest;
}

namespace gpopt
{
	class CEnumeratorConfig;
	class CStatisticsConfig;
	class COptimizerConfig;
	class ICostModel;
}

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE

	// fwd decl
	class CParseHandlerDXL;
	class CDXLMemoryManager;
	class CQueryToDXLResult;
	
	typedef CDynamicPtrArray<CStatistics, CleanupRelease> DrgPstats;
	
	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLUtils
	//
	//	@doc:
	//		Entry point for parsing and serializing DXL documents.
	//
	//---------------------------------------------------------------------------
	class CDXLUtils 
	{
			
		private:

			// same as above but with a wide string parameter for the DXL document
			static 
			CParseHandlerDXL *PphdxlParseDXL
				(
				IMemoryPool *,
				const CWStringBase *pstr,
				const CHAR *szXSDPath
				);
			


		public:
			// helper functions for serializing DXL document header and footer, respectively
			static void SerializeHeader(IMemoryPool *, CXMLSerializer *);
			static void SerializeFooter(CXMLSerializer *);
			// helper routine which initializes and starts the xerces parser, 
			// and returns the top-level parse handler which can be used to
			// retrieve the parsed elements
			static 
			CParseHandlerDXL *PphdxlParseDXL
				(
				IMemoryPool *,
				const CHAR *szDXL,
				const CHAR *szXSDPath
				);
			
			// same as above but with DXL file name specified instead of the file contents
			static 
			CParseHandlerDXL *PphdxlParseDXLFile
				(
				IMemoryPool *,
				const CHAR *szDXLFileName,
				const CHAR *szXSDPath
				);

			// parse a DXL document containing a DXL plan
			static 
			CDXLNode *PdxlnParsePlan
				(
				IMemoryPool *,
				const CHAR *szDXL,
				const CHAR *szXSDPath,
				ULLONG *pullPlanId,
				ULLONG *pullPlanSpaceSize
				);
			
			// parse a DXL document representing a query
			// to return the DXL tree representing the query and
			// a DXL tree representing the query output
			static 
			CQueryToDXLResult *PdxlnParseDXLQuery
				(
				IMemoryPool *,
				const CHAR *szDXL,
				const CHAR *szXSDPath
				);
			
			// parse a DXL document containing a scalar expression
			static
			CDXLNode *PdxlnParseScalarExpr
				(
				IMemoryPool *,
				const CHAR *szDXL,
				const CHAR *szXSDPath
				);

			// parse an MD request
			static 
			CMDRequest *PmdrequestParseDXL
				(
				IMemoryPool *pmp,
				const CHAR *szDXL,
				const CHAR *szXSDPath
				);
			
			// parse a list of mdids from a MD request message
			static 
			CMDRequest *PmdrequestParseDXL
				(
				IMemoryPool *pmp,
				const WCHAR *wszDXL,
				const CHAR *szXSDPath
				);

			// parse optimizer config DXL
			static
			COptimizerConfig *PoptimizerConfigParseDXL
				(
				IMemoryPool *pmp,
				const CHAR *szDXL,
				const CHAR *szXSDPath
				);

			static 
			DrgPimdobj *PdrgpmdobjParseDXL
				(
				IMemoryPool *,
				const CHAR *szDXL,
				const CHAR *szXSDPath
				);
			
			static 
			DrgPimdobj *PdrgpmdobjParseDXL
				(
				IMemoryPool *,
				const CWStringBase *pstrDXL,
				const CHAR *szXSDPath
				);
			
			// parse mdid from a metadata document
			static 
			IMDId *PmdidParseDXL
				(
				IMemoryPool *,
				const CWStringBase *pstrDXL,
				const CHAR *szXSDPath
				);

			static 
			IMDCacheObject *PimdobjParseDXL
				(
				IMemoryPool *,
				const CWStringBase *pstrDXL,
				const CHAR *szXSDPath
				);

			// parse statistics object from the statistics document
			static 
			DrgPdxlstatsderrel *PdrgpdxlstatsderrelParseDXL
				(
				IMemoryPool *,
				const CHAR *szDXL,
				const CHAR *szXSDPath
				);

			// parse statistics object from the statistics document
			static 
			DrgPdxlstatsderrel *PdrgpdxlstatsderrelParseDXL
				(
				IMemoryPool *,
				const CWStringBase *pstrDXL,
				const CHAR *szXSDPath
				);
			
			// translate the dxl statistics object to optimizer statistics object
			static
			DrgPstats *PdrgpstatsTranslateStats
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				DrgPdxlstatsderrel *pdrgpdxlstatsderrel
				);

			// extract the array of optimizer buckets from the dxl representation of
			// dxl buckets in the dxl derived column statistics object
			static
			DrgPbucket *Pdrgpbucket
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				CDXLStatsDerivedColumn *pdxlstatsdercol
				);

			// serialize a DXL query tree into DXL Document
			static 
			void SerializeQuery
				(
				IMemoryPool *pmp,
				IOstream& os,
				const CDXLNode *pdxlnQuery,
				const DrgPdxln *pdrgpdxlnQueryOutput,
				const DrgPdxln *pdrgpdxlnCTE,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);
			
			// serialize a ULLONG value
			static
			CWStringDynamic *PstrSerializeULLONG
				(
				IMemoryPool *pmp,
				ULLONG ullVal
				);

			// serialize a plan into DXL
			static 
			void SerializePlan
				(
				IMemoryPool *pmp,
				IOstream &os,
				const CDXLNode *pdxln,
				ULLONG ullPlanId,
				ULLONG ullPlanSpaceSize,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);

			static 
			CWStringDynamic *PstrSerializeStatistics
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				const DrgPstats *pdrgpstat,
				BOOL fSerializeHeaderFooter,
				BOOL fIndent
				);

			// serialize statistics objects into DXL and write to stream
			static 
			void SerializeStatistics
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				const DrgPstats *pdrgpstat,
				IOstream &os,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);

			// serialize metadata objects into DXL and write to stream
			static 
			void SerializeMetadata
				(
				IMemoryPool *pmp,
				const DrgPimdobj *pdrgpmdobj,
				IOstream &os,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);

			// serialize metadata ids into a MD request message
			static 
			void SerializeMDRequest
				(
				IMemoryPool *pmp,
				CMDRequest *pmdr,
				IOstream &os,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);
			
			// serialize a metadata id into a MD request message
			static 
			void SerializeMDRequest
				(
				IMemoryPool *pmp,
				const IMDId *pmdid,
				IOstream &os,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);

			// serialize a list of metadata objects into DXL
			static 
			CWStringDynamic *PstrSerializeMetadata
				(
				IMemoryPool *,
				const DrgPimdobj *,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);
			
			// serialize a metadata id into DXL
			static 
			CWStringDynamic *PstrSerializeMetadata
				(
				IMemoryPool *pmp,
				const IMDId *pmdid,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);

			// serialize sample plans
			static
			CWStringDynamic *PstrSerializeSamplePlans
				(
				IMemoryPool *pmp,
				CEnumeratorConfig *pec,
				BOOL fIndent
				);

			// serialize cost distribution plans
			static
			CWStringDynamic *PstrSerializeCostDistr
				(
				IMemoryPool *pmp,
				CEnumeratorConfig *pec,
				BOOL fIndent
				);

			// serialize a metadata object into DXL
			static 
			CWStringDynamic *PstrSerializeMDObj
				(
				IMemoryPool *,
				const IMDCacheObject *,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);
			
			// serialize a scalar expression into DXL
			static
			CWStringDynamic *PstrSerializeScalarExpr
				(
				IMemoryPool *pmp,
				const CDXLNode *pdxln,
				BOOL fDocumentHeaderFooter,
				BOOL fIndent
				);

			// create a GPOS dynamic string from a Xerces XMLCh array
			static 
			CWStringDynamic *PstrFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *
				);
			
			// create a GPOS string object from a base 64 encoded XML string
			static
			BYTE *PbaFromBase64XMLStr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				ULONG *pulLength
				);

			// create a GPOS dynamic string from a regular character array
			static 
			CWStringDynamic *PstrFromSz
				(
				IMemoryPool *pmp,
				const CHAR *sz
				);
			
			// create an MD name from a character array
			static 
			CMDName *PmdnameFromSz(IMemoryPool *pmp, const CHAR *sz);	
			
			// create an MD name from a Xerces character array
			static 
			CMDName *PmdnameFromXmlsz(CDXLMemoryManager *pmm, const XMLCh *xmlsz);
		
			// encode a byte array to a string
			static 
			CWStringDynamic *PstrFromByteArray
				(
				IMemoryPool *pmp,
				const BYTE *pba,
				ULONG ulLength
				);

			// serialize a list of integers into a comma-separate string
			template <typename T, void (*pfnDestroy)(T*)>
			static 
			CWStringDynamic *PstrSerialize
				(
				IMemoryPool *pmp,
				const CDynamicPtrArray<T, pfnDestroy> *pdrgpt
				);

			// serialize a list of lists of integers into a comma-separate string
			static
			CWStringDynamic *PstrSerialize
				(
				IMemoryPool *pmp,
				const DrgPdrgPul *pdrgpul
				);

			// serialize a list of chars into a comma-separate string
			static
			CWStringDynamic *PstrSerializeSz
				(
				IMemoryPool *pmp,
				const DrgPsz *pdrgpsz
				);

			// decode a byte array from a string
			static 
			BYTE *PByteArrayFromStr
				(
				IMemoryPool *pmp,
				const CWStringDynamic *pstr,
				ULONG *pulLength
				);
			
			static 
			CHAR *SzRead(IMemoryPool *pmp, const CHAR *szFileName);
			
			// create a multi-byte character string from a wide character string
			static 
			CHAR *SzFromWsz(IMemoryPool *pmp, const WCHAR *wsz);
			
			// serialize a double value in a string
			static 
			CWStringDynamic *PstrFromDouble
				(
				CDXLMemoryManager *pmm,
				CDouble dValue
				);

			// translate the optimizer datum from dxl datum object
			static
			IDatum *Pdatum
				(
				IMemoryPool *pmp,
				CMDAccessor *pmda,
				const CDXLDatum *Pdxldatum
				);

			// serialize Datum with given tag
			static
			void SerializeBound
					(
					IDatum *pdatum,
					const CWStringConst *pstr,
					CXMLSerializer *pxmlser
					);

#ifdef GPOS_DEBUG
			// debug print of the metadata relation
			static
			void DebugPrintDrgpmdid(IOstream &os, DrgPmdid *pdrgpmdid);
#endif
	};

	// serialize a list of integers into a comma-separate string
	template <typename T, void (*pfnDestroy)(T*)>
	CWStringDynamic *
	CDXLUtils::PstrSerialize
		(
		IMemoryPool *pmp,
		const CDynamicPtrArray<T, pfnDestroy> *pdrgpt
		)
	{
		CAutoP<CWStringDynamic> a_pstr(GPOS_NEW(pmp) CWStringDynamic(pmp));

		ULONG ulLength = pdrgpt->UlLength();
		for (ULONG ul = 0; ul < ulLength; ul++)
		{
			T tValue = *((*pdrgpt)[ul]);
			if (ul == ulLength - 1)
			{
				// last element: do not print a comma
				a_pstr->AppendFormat(GPOS_WSZ_LIT("%d"), tValue);
			}
			else
			{
				a_pstr->AppendFormat(GPOS_WSZ_LIT("%d%ls"), tValue, CDXLTokens::PstrToken(EdxltokenComma)->Wsz());
			}
		}

		return a_pstr.PtReset();
	}

}

#endif // GPDXL_CDXLUtils_H

// EOF
