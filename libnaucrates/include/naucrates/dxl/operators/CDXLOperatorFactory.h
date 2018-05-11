//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CDXLOperatorFactory.h
//
//	@doc:
//		Factory for creating DXL tree elements out of parsed XML attributes
//---------------------------------------------------------------------------

#ifndef GPDXL_CDXLOperatorFactory_H
#define GPDXL_CDXLOperatorFactory_H

#include <xercesc/util/XMLUniDefs.hpp>
#include <xercesc/sax2/Attributes.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/util/XMLStringTokenizer.hpp>

#include "gpos/base.h"
#include "naucrates/dxl/operators/dxlops.h"
#include "naucrates/dxl/xml/CDXLMemoryManager.h"
#include "naucrates/dxl/xml/dxltokens.h"
#include "naucrates/md/IMDId.h"
#include "naucrates/md/CMDFunctionGPDB.h"
#include "naucrates/md/CMDRelationGPDB.h"
#include "naucrates/md/IMDIndex.h"
#include "naucrates/base/IDatum.h"
#include "gpos/common/CDouble.h"

// dynamic array of XML strings
typedef CDynamicPtrArray<XMLCh, CleanupNULL> DrgPxmlsz;

// fwd decl
namespace gpmd
{
	class CMDIdGPDB;
	class CMDIdColStats;
	class CMDIdRelStats;
	class CMDIdCast;
	class CMDIdScCmp;
}

namespace gpdxl
{
	using namespace gpos;
	using namespace gpmd;
	using namespace gpnaucrates;

	XERCES_CPP_NAMESPACE_USE
	
	//fwd decl
	class CDXLMemoryManager;
	class CDXLDatum;

	// shorthand for functions for translating a DXL datum
	typedef CDXLDatum* (PfPdxldatum) (CDXLMemoryManager *, const Attributes &, Edxltoken , IMDId *, BOOL , BOOL);

	//---------------------------------------------------------------------------
	//	@class:
	//		CDXLOperatorFactory
	//
	//	@doc:
	//		Factory class containing static methods for creating DXL objects
	//		from parsed DXL information such as XML element's attributes
	//
	//---------------------------------------------------------------------------
	class CDXLOperatorFactory
	{
			
		private:

			// return the LINT value of byte array
			static
			LINT LValue
				(
				CDXLMemoryManager *pmm,
				const Attributes &attrs,
				Edxltoken edxltokenElement,
				BYTE *pba
				);

			// parses a byte array representation of the datum
			static
			BYTE *Pba
					(
					CDXLMemoryManager *pmm,
					const Attributes &attrs,
					Edxltoken edxltokenElement,
					ULONG *pulLength
					);

		public:

			// pair of oid for datums and the factory function
			struct SDXLDatumFactoryElem
			{
				OID oid;
				PfPdxldatum *pf;
			};

			static
			CDXLDatum *PdxldatumOid
								(
								CDXLMemoryManager *pmm,
								const Attributes &attrs,
								Edxltoken edxltokenElement,
								IMDId *pmdid,
								BOOL fConstNull ,
								BOOL fConstByVal
								);

			static
			CDXLDatum *PdxldatumInt2
								(
								CDXLMemoryManager *pmm,
								const Attributes &attrs,
								Edxltoken edxltokenElement,
								IMDId *pmdid,
								BOOL fConstNull ,
								BOOL fConstByVal
								);

			static
			CDXLDatum *PdxldatumInt4
								(
								CDXLMemoryManager *pmm,
								const Attributes &attrs,
								Edxltoken edxltokenElement,
								IMDId *pmdid,
								BOOL fConstNull ,
								BOOL fConstByVal
								);

			static
			CDXLDatum *PdxldatumInt8
								(
								CDXLMemoryManager *pmm,
								const Attributes &attrs,
								Edxltoken edxltokenElement,
								IMDId *pmdid,
								BOOL fConstNull ,
								BOOL fConstByVal
								);

			static
			CDXLDatum *PdxldatumBool
								(
								CDXLMemoryManager *pmm,
								const Attributes &attrs,
								Edxltoken edxltokenElement,
								IMDId *pmdid,
								BOOL fConstNull ,
								BOOL fConstByVal
								);

			// parse a dxl datum of type generic
			static
			CDXLDatum *PdxldatumGeneric
								(
								CDXLMemoryManager *pmm,
								const Attributes &attrs,
								Edxltoken edxltokenElement,
								IMDId *pmdid,
								BOOL fConstNull ,
								BOOL fConstByVal
								);

			// parse a dxl datum of types that need double mapping
			static
			CDXLDatum *PdxldatumStatsDoubleMappable
								(
								CDXLMemoryManager *pmm,
								const Attributes &attrs,
								Edxltoken edxltokenElement,
								IMDId *pmdid,
								BOOL fConstNull ,
								BOOL fConstByVal
								);

			// parse a dxl datum of types that need lint mapping
			static
			CDXLDatum *PdxldatumStatsLintMappable
								(
								CDXLMemoryManager *pmm,
								const Attributes &attrs,
								Edxltoken edxltokenElement,
								IMDId *pmdid,
								BOOL fConstNull ,
								BOOL fConstByVal
								);

			// create a table scan operator
			static
			CDXLPhysical *PdxlopTblScan(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a subquery scan operator
			static
			CDXLPhysical *PdxlopSubqScan(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a result operator
			static
			CDXLPhysical *PdxlopResult(CDXLMemoryManager *pmm);

			// create a hashjoin operator
			static
			CDXLPhysical *PdxlopHashJoin(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a nested loop join operator
			static
			CDXLPhysical *PdxlopNLJoin(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a merge join operator
			static
			CDXLPhysical *PdxlopMergeJoin(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a gather motion operator
			static
			CDXLPhysical *PdxlopGatherMotion(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a broadcast motion operator
			static
			CDXLPhysical *PdxlopBroadcastMotion(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a redistribute motion operator
			static
			CDXLPhysical *PdxlopRedistributeMotion(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a routed motion operator
			static
			CDXLPhysical *PdxlopRoutedMotion(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a random motion operator
			static
			CDXLPhysical *PdxlopRandomMotion(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create an append operator
			static
			CDXLPhysical *PdxlopAppend(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a limit operator
			static
			CDXLPhysical *PdxlopLimit(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create an aggregation operator
			static
			CDXLPhysical *PdxlopAgg(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a sort operator
			static
			CDXLPhysical *PdxlopSort(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a materialize operator
			static
			CDXLPhysical *PdxlopMaterialize(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a limit count operator
			static
			CDXLScalar *PdxlopLimitCount(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a limit offset operator
			static
			CDXLScalar *PdxlopLimitOffset(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a scalar comparison operator
			static
			CDXLScalar *PdxlopScalarCmp(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a distinct comparison operator
			static
			CDXLScalar *PdxlopDistinctCmp(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a scalar OpExpr
			static
			CDXLScalar *PdxlopOpExpr(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a scalar ArrayComp
			static
			CDXLScalar *PdxlopArrayComp(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a BoolExpr
			static
			CDXLScalar *PdxlopBoolExpr(CDXLMemoryManager *pmm, const EdxlBoolExprType);

			// create a boolean test
			static
			CDXLScalar *PdxlopBooleanTest(CDXLMemoryManager *pmm, const EdxlBooleanTestType);

			// create a subplan operator
			static
			CDXLScalar *PdxlopSubPlan
				(
				CDXLMemoryManager *pmm,
				IMDId *pmdid,
				DrgPdxlcr *pdrgdxlcr,
				EdxlSubPlanType edxlsubplantype,
				CDXLNode *pdxlnTestExpr
				);

			// create a NullTest
			static
			CDXLScalar *PdxlopNullTest(CDXLMemoryManager *pmm, const BOOL );

			// create a cast
			static
			CDXLScalar *PdxlopCast(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a coerce
			static
			CDXLScalar *PdxlopCoerceToDomain(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a CoerceViaIo
			static
			CDXLScalar *PdxlopCoerceViaIO(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a ArrayCoerceExpr
			static
			CDXLScalar *PdxlopArrayCoerceExpr(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a scalar identifier operator
			static
			CDXLScalar *PdxlopScalarIdent(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a scalar Const
			static
			CDXLScalar *PdxlopConstValue(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a CaseStmt
			static
			CDXLScalar *PdxlopIfStmt(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a FuncExpr
			static
			CDXLScalar *PdxlopFuncExpr(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a AggRef
			static
			CDXLScalar *PdxlopAggFunc(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a scalar window function (WindowRef)
			static
			CDXLScalar *PdxlopWindowRef(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create an array
			static
			CDXLScalar *PdxlopArray(CDXLMemoryManager *pmm, const Attributes &attr);

			// create a proj elem
			static
			CDXLScalar *PdxlopProjElem(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a hash expr 
			static
			CDXLScalar *PdxlopHashExpr(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a sort col
			static
			CDXLScalar *PdxlopSortCol(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create an object representing cost estimates of a physical operator
			// from the parsed XML attributes
			static
			CDXLOperatorCost *Pdxlopcost(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a table descriptor element
			static
			CDXLTableDescr *Pdxltabdesc(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create an index descriptor
			static
			CDXLIndexDescr *Pdxlid(CDXLMemoryManager *pmm, const Attributes &attrs);

			// create a column descriptor object
			static
			CDXLColDescr *Pdxlcd(CDXLMemoryManager *pmm, const Attributes &attrs);
			
			// create a column reference object
			static
			CDXLColRef *Pdxlcr(CDXLMemoryManager *pmm, const Attributes &, Edxltoken);
			
			// create a logical join 
			static
			CDXLLogical *PdxlopLogicalJoin(CDXLMemoryManager *pmm, const Attributes &attrs);

			// parse an output segment index
			static
			INT IOutputSegId(CDXLMemoryManager *pmm, const Attributes &attrs);

			// parse a grouping column id
			static
			ULONG UlGroupingColId(CDXLMemoryManager *pmm, const Attributes &attrs);

			// extracts the value for the given attribute.
			// if there is no such attribute defined, and the given optional
			// flag is set to false then it will raise an exception
			static
			const XMLCh *XmlstrFromAttrs
				(
				const Attributes &,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false
				);

			// extracts the boolean value for the given attribute
			// will raise an exception if value cannot be converted to a boolean
			static
			BOOL FValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// converts the XMLCh into LINT
			static
			LINT LValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// extracts the LINT value for the given attribute
			static
			LINT LValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				LINT lDefaultValue = 0
				);

			// converts the XMLCh into CDouble
			static
			CDouble DValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// cxtracts the double value for the given attribute
			static
			CDouble DValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// converts the XMLCh into ULONG. Will raise an exception if the 
			// argument cannot be converted to ULONG
			static
			ULONG UlValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// converts the XMLCh into ULLONG. Will raise an exception if the
			// argument cannot be converted to ULLONG
			static
			ULLONG UllValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// converts the XMLCh into INT. Will raise an exception if the 
			// argument cannot be converted to INT
			static
			INT IValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// parse a INT value from the value for a given attribute
			// will raise an exception if the argument cannot be converted to INT
			static
			INT IValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				INT iDefaultValue = 0
				);

			// converts the XMLCh into short int. Will raise an exception if the
			// argument cannot be converted to short int
			static
			SINT SValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// parse a short int value from the value for a given attribute
			// will raise an exception if the argument cannot be converted to short int
			static
			SINT SValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				SINT sDefaultValue = 0
				);

			// converts the XMLCh into char. Will raise an exception if the
			// argument cannot be converted to char
			static
			CHAR CValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// converts the XMLCh into oid. Will raise an exception if the
			// argument cannot be converted to OID
			static
			OID OidValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// parse an oid value from the value for a given attribute
			// will raise an exception if the argument cannot be converted to OID
			static
			OID OidValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				OID OidDefaultValue = 0
				);

			// parse a bool value from the value for a given attribute
			static
			BOOL FValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				BOOL fDefaultValue = false
				);
			
			// parse a string value from the value for a given attribute
			static
			CHAR *SzValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				CHAR *szDefaultValue = NULL
				);

			// parse a string value from the value for a given attribute
			static
			CHAR* SzValueFromXmlstr
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			// parse a string value from the value for a given attribute
			static
			CWStringDynamic *PstrValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			// parse a ULONG value from the value for a given attribute
			// will raise an exception if the argument cannot be converted to ULONG
			static
			ULONG UlValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				ULONG ulDefaultValue = 0
				);
			
			// parse a ULLONG value from the value for a given attribute
			// will raise an exception if the argument cannot be converted to ULLONG
			static
			ULLONG UllValueFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				ULLONG ullDefaultValue = 0
				);

			// parse an mdid object from the given attributes
			static
			IMDId *PmdidFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement,
				BOOL fOptional = false,
				IMDId *pmdidDefault = NULL
				);
			
			// parse an mdid object from an XMLCh
			static
			IMDId *PmdidFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlszMdid,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			// parse a GPDB mdid object from an array of its components
			static
			CMDIdGPDB *PmdidGPDB
				(
				CDXLMemoryManager *pmm,
				DrgPxmlsz *pdrgpxmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			// parse a GPDB CTAS mdid object from an array of its components
			static
			CMDIdGPDB *PmdidGPDBCTAS
				(
				CDXLMemoryManager *pmm,
				DrgPxmlsz *pdrgpxmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// parse a column stats mdid object from an array of its components
			static
			CMDIdColStats *PmdidColStats
				(
				CDXLMemoryManager *pmm,
				DrgPxmlsz *pdrgpxmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// parse a relation stats mdid object from an array of its components
			static
			CMDIdRelStats *PmdidRelStats
				(
				CDXLMemoryManager *pmm,
				DrgPxmlsz *pdrgpxmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			// parse a cast func mdid from the array of its components
			static
			CMDIdCast *PmdidCastFunc
				(
				CDXLMemoryManager *pmm,
				DrgPxmlsz *pdrgpxmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				); 
			
			// parse a comparison operator mdid from the array of its components
			static
			CMDIdScCmp *PmdidScCmp
				(
				CDXLMemoryManager *pmm,
				DrgPxmlsz *pdrgpxmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			// parse a dxl datum object
			static
			CDXLDatum *Pdxldatum
				(
				CDXLMemoryManager *pmm,
				const Attributes &attrs,
				Edxltoken edxltokenElement
				);
			
			// parse a comma-separated list of MDids into a dynamic array
			// will raise an exception if list is not well-formed
			static
			DrgPmdid *PdrgpmdidFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlszUlList,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// parse a comma-separated list of unsigned long numbers into a dynamic array
			// will raise an exception if list is not well-formed
			static
			DrgPul *PdrgpulFromAttrs
				(
				CDXLMemoryManager *pmm,
				const Attributes &attr,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// parse a comma-separated list of integers numbers into a dynamic array
			// will raise an exception if list is not well-formed
			template <typename T, void (*pfnDestroy)(T*),
					T ValueFromXmlstr(CDXLMemoryManager *, const XMLCh *, Edxltoken, Edxltoken)>
			static
			CDynamicPtrArray<T, pfnDestroy> *PdrgptFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlszUl,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			static
			DrgPul *PdrgpulFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlszUl,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				)
			{
				return PdrgptFromXMLCh<ULONG, CleanupDelete, UlValueFromXmlstr>
						(
						pmm,
						xmlszUl,
						edxltokenAttr,
						edxltokenElement
						);
			}

			static
			DrgPi *PdrgpiFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlszUl,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				)
			{
				return PdrgptFromXMLCh<INT, CleanupDelete, IValueFromXmlstr>
						(
						pmm,
						xmlszUl,
						edxltokenAttr,
						edxltokenElement
						);
			}

			// parse a comma-separated list of CHAR partition types into a dynamic array.
			// will raise an exception if list is not well-formed
			static
			DrgPsz *PdrgpszFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);

			// parse a semicolon-separated list of comma-separated unsigned 
			// long numbers into a dynamc array of unsigned integer arrays
			// will raise an exception if list is not well-formed
			static
			DrgPdrgPul *PdrgpdrgpulFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			// parse a comma-separated list of segment ids into a dynamic array
			// will raise an exception if list is not well-formed
			static
			DrgPi *PdrgpiParseSegmentIdList
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlszSegIdList,
				Edxltoken edxltokenAttr,
				Edxltoken edxltokenElement
				);
			
			// parse a comma-separated list of strings into a dynamic array
			// will raise an exception if list is not well-formed
			static
			DrgPstr *PdrgPstrFromXMLCh
				(
				CDXLMemoryManager *pmm,
				const XMLCh *xmlsz
				);
			
			// parses the input and output segment ids from Xerces attributes and
			// stores them in the provided DXL Motion operator
			// will raise an exception if lists are not well-formed 
			static
			void SetSegmentInfo
				(
				CDXLMemoryManager *pmp,
				CDXLPhysicalMotion *pdxlopMotion,
				const Attributes &attrs,
				Edxltoken edxltokenElement
				);
			
			static
			EdxlJoinType EdxljtParseJoinType(const XMLCh *xmlszJoinType, const CWStringConst *pstrJoinName);
			
			static
			EdxlIndexScanDirection EdxljtParseIndexScanDirection
				(
				const XMLCh *xmlszIndexScanDirection,
				const CWStringConst *pstrIndexScanDirection
				);

			// parse system id
			static
			CSystemId Sysid(CDXLMemoryManager *pmm, const Attributes &attrs, Edxltoken edxltokenAttr, Edxltoken edxltokenElement);
			
			// parse the frame boundary
			static
			EdxlFrameBoundary Edxlfb(const Attributes& attrs, Edxltoken edxltoken);

			// parse the frame specification
			static
			EdxlFrameSpec Edxlfs(const Attributes& attrs);

			// parse the frame exclusion strategy
			static
			EdxlFrameExclusionStrategy Edxlfes(const Attributes& attrs);
			
			// parse comparison operator type
			static
			IMDType::ECmpType Ecmpt(const XMLCh* xmlszCmpType);
			
			// parse the distribution policy from the given XML string
			static
			IMDRelation::Ereldistrpolicy EreldistrpolicyFromXmlstr(const XMLCh *xmlsz);
			
			// parse the storage type from the given XML string
			static
			IMDRelation::Erelstoragetype ErelstoragetypeFromXmlstr(const XMLCh *xmlsz);
			
			// parse the OnCommit action spec for CTAS
			static
			CDXLCtasStorageOptions::ECtasOnCommitAction EctascommitFromAttr(const Attributes &attr);
			
			// parse index type
			static
			IMDIndex::EmdindexType EmdindtFromAttr(const Attributes &attrs);

	};

	// parse a comma-separated list of integers numbers into a dynamic array
	// will raise an exception if list is not well-formed
	template <typename T, void (*pfnDestroy)(T*),
			T ValueFromXmlstr(CDXLMemoryManager *, const XMLCh *, Edxltoken, Edxltoken)>
	CDynamicPtrArray<T, pfnDestroy> *
	CDXLOperatorFactory::PdrgptFromXMLCh
		(
		CDXLMemoryManager *pmm,
		const XMLCh *xmlszUlList,
		Edxltoken edxltokenAttr,
		Edxltoken edxltokenElement
		)
	{
		// get the memory pool from the memory manager
		IMemoryPool *pmp = pmm->Pmp();

		CDynamicPtrArray<T, pfnDestroy> *pdrgpt = GPOS_NEW(pmp) CDynamicPtrArray<T, pfnDestroy>(pmp);

		XMLStringTokenizer xmlsztok(xmlszUlList, CDXLTokens::XmlstrToken(EdxltokenComma));
		const ULONG ulNumTokens = xmlsztok.countTokens();

		for (ULONG ul = 0; ul < ulNumTokens; ul++)
		{
			XMLCh *xmlszNext = xmlsztok.nextToken();

			GPOS_ASSERT(NULL != xmlszNext);

			T *pt = GPOS_NEW(pmp) T(ValueFromXmlstr(pmm, xmlszNext, edxltokenAttr, edxltokenElement));
			pdrgpt->Append(pt);
		}

		return pdrgpt;
	}
}

#endif // !GPDXL_CDXLOperatorFactory_H

// EOF
