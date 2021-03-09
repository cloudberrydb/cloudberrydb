//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2014 VMware, Inc. or its affiliates.
//
//	@filename:
//		CParseHandlerPartitionSelector.h
//
//	@doc:
//		SAX parse handler class for parsing a partition selector
//---------------------------------------------------------------------------

#ifndef GPDXL_CParseHandlerScalarPartitionSelector_H
#define GPDXL_CParseHandlerScalarPartitionSelector_H

#include "gpos/base.h"

#include "naucrates/dxl/parser/CParseHandlerPhysicalOp.h"

namespace gpdxl
{
using namespace gpos;

XERCES_CPP_NAMESPACE_USE

//---------------------------------------------------------------------------
//	@class:
//		CParseHandlerPartitionSelector
//
//	@doc:
//		Parse handler class for parsing a partition selector
//
//---------------------------------------------------------------------------
class CParseHandlerPartitionSelector : public CParseHandlerPhysicalOp
{
private:
	// table id
	IMDId *m_rel_mdid;

	// number of partitioning levels
	ULONG m_selector_id;

	// scan id
	ULONG m_scan_id;

	// partitions
	ULongPtrArray *m_partitions;

	// process the start of an element
	void StartElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname,		// element's qname
		const Attributes &attr					// element's attributes
		) override;

	// process the end of an element
	void EndElement(
		const XMLCh *const element_uri,			// URI of element's namespace
		const XMLCh *const element_local_name,	// local part of element's name
		const XMLCh *const element_qname		// element's qname
		) override;

public:
	CParseHandlerPartitionSelector(const CParseHandlerPartitionSelector &) =
		delete;

	// ctor
	CParseHandlerPartitionSelector(CMemoryPool *mp,
								   CParseHandlerManager *parse_handler_mgr,
								   CParseHandlerBase *parse_handler_root);

	~CParseHandlerPartitionSelector() override;
};
}  // namespace gpdxl

#endif	// !GPDXL_CParseHandlerScalarPartitionSelector_H

// EOF
