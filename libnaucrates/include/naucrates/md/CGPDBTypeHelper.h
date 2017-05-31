//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2011 EMC Corp.
//
//	@filename:
//		CGPDBTypeHelper.h
//
//	@doc:
//		Helper class that provides implementation for common functions across
//		different GPDB types (CMDTypeInt4GPDB, CMDTypeBoolGPDB, and CMDTypeGenericGPDB)
//---------------------------------------------------------------------------
#ifndef GPMD_CGPDBHelper_H
#define GPMD_CGPDBHelper_H

#include "gpos/base.h"

#include "naucrates/dxl/xml/CXMLSerializer.h"
#include "naucrates/dxl/CDXLUtils.h"


namespace gpmd
{
	using namespace gpos;
	using namespace gpdxl;

	template<class T>
	class CGPDBTypeHelper
	{

		public:

			// serialize object in DXL format
			static void Serialize(CXMLSerializer *pxmlser, const T *pt)
            {
                pxmlser->OpenElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
                                     CDXLTokens::PstrToken(EdxltokenMDType));

                pt->Pmdid()->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMdid));

                pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenName), pt->Mdname().Pstr());
                pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMDTypeRedistributable), pt->FRedistributable());
                pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMDTypeHashable), pt->FHashable());
                pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMDTypeComposite), pt->FComposite());

                if (pt->FComposite())
                {
                    pt->PmdidBaseRelation()->Serialize(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeRelid));
                }

                pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMDTypeFixedLength), pt->FFixedLength());

                if (pt->FFixedLength())
                {
                    pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMDTypeLength), pt->UlLength());
                }

                pxmlser->AddAttribute(CDXLTokens::PstrToken(EdxltokenMDTypeByValue), pt->FByValue());

                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeEqOp), pt->PmdidCmp(IMDType::EcmptEq));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeNEqOp), pt->PmdidCmp(IMDType::EcmptNEq));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeLTOp), pt->PmdidCmp(IMDType::EcmptL));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeLEqOp), pt->PmdidCmp(IMDType::EcmptLEq));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeGTOp), pt->PmdidCmp(IMDType::EcmptG));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeGEqOp), pt->PmdidCmp(IMDType::EcmptGEq));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeCompOp), pt->PmdidOpComp());
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeArray), pt->PmdidTypeArray());

                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeAggMin), pt->PmdidAgg(IMDType::EaggMin));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeAggMax), pt->PmdidAgg(IMDType::EaggMax));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeAggAvg), pt->PmdidAgg(IMDType::EaggAvg));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeAggSum), pt->PmdidAgg(IMDType::EaggSum));
                pt->SerializeMDIdAsElem(pxmlser, CDXLTokens::PstrToken(EdxltokenMDTypeAggCount), pt->PmdidAgg(IMDType::EaggCount));

                pxmlser->CloseElement(CDXLTokens::PstrToken(EdxltokenNamespacePrefix),
                                      CDXLTokens::PstrToken(EdxltokenMDType));

                GPOS_CHECK_ABORT;
            }

#ifdef GPOS_DEBUG
			// debug print of the type in the provided stream
			static void DebugPrint(IOstream &os, const T *pt)
            {
                os << "Type id: ";
                pt->Pmdid()->OsPrint(os);
                os << std::endl;

                os << "Type name: " << pt->Mdname().Pstr()->Wsz() << std::endl;

                const CWStringConst *pstrRedistributable = pt->FRedistributable() ?
                CDXLTokens::PstrToken(EdxltokenTrue):
                CDXLTokens::PstrToken(EdxltokenFalse);

                os << "Redistributable: " << pstrRedistributable->Wsz() << std::endl;

                const CWStringConst *pstrFixedLength = pt->FFixedLength() ?
                CDXLTokens::PstrToken(EdxltokenTrue):
                CDXLTokens::PstrToken(EdxltokenFalse);

                os << "Fixed length: " << pstrFixedLength->Wsz() << std::endl;

                if (pt->FFixedLength())
                {
                    os << "Type length: " << pt->UlLength() << std::endl;
                }

                const CWStringConst *pstrByValue = pt->FByValue() ?
                CDXLTokens::PstrToken(EdxltokenTrue):
                CDXLTokens::PstrToken(EdxltokenFalse);

                os << "Pass by value: " << pstrByValue->Wsz() << std::endl;

                os << "Equality operator id: ";
                pt->PmdidCmp(IMDType::EcmptEq)->OsPrint(os);
                os << std::endl;

                os << "Less-than operator id: ";
                pt->PmdidCmp(IMDType::EcmptL)->OsPrint(os);
                os << std::endl;

                os << "Greater-than operator id: ";
                pt->PmdidCmp(IMDType::EcmptG)->OsPrint(os);
                os << std::endl;

                os << "Comparison operator id: ";
                pt->PmdidOpComp()->OsPrint(os);
                os << std::endl;

                os << std::endl;
            }
#endif // GPOS_DEBUG
	};
}

#endif // !CGPMD_GPDBHelper_H

// EOF

