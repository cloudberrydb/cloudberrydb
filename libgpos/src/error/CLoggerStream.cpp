//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2010 Greenplum, Inc.
//
//	@filename:
//		CLoggerStream.cpp
//
//	@doc:
//		Implementation of stream logging
//---------------------------------------------------------------------------

#include "gpos/error/CLoggerStream.h"

using namespace gpos;

CLoggerStream CLoggerStream::m_plogStdOut(oswcout);
CLoggerStream CLoggerStream::m_plogStdErr(oswcerr);


//---------------------------------------------------------------------------
//	@function:
//		CLoggerStream::CLoggerStream
//
//	@doc:
//
//---------------------------------------------------------------------------
CLoggerStream::CLoggerStream
	(
	IOstream &os
	)
	:
	CLogger(),
	m_os(os)
{}

//---------------------------------------------------------------------------
//	@function:
//		CLoggerStream::~CLoggerStream
//
//	@doc:
//
//---------------------------------------------------------------------------
CLoggerStream::~CLoggerStream()
{}


// EOF

