//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 - 2010 Greenplum, Inc.
//
//	@filename:
//		CMessage.cpp
//
//	@doc:
//		Implements message records
//---------------------------------------------------------------------------

#include "gpos/utils.h"
#include "gpos/error/CMessage.h"
#include "gpos/error/CMessageRepository.h"

using namespace gpos;

//---------------------------------------------------------------------------
//	@function:
//		CMessage::CMessage
//
//	@doc:
//
//---------------------------------------------------------------------------
CMessage::CMessage
	(
	CException exc, 
	ULONG ulSev,
	const WCHAR *wszFmt, ULONG ulLenFmt, 
	ULONG ulParams,
	const WCHAR *wszComment, ULONG ulLenComment
	)
	:
	m_ulSev(ulSev),
	m_wszFmt(wszFmt),
	m_ulLenFmt(ulLenFmt),
	m_ulParams(ulParams),
	m_wszComment(wszComment),
	m_ulLenComment(ulLenComment),
	m_exc(exc)
{
	// TODO: 6/29/2010; incorporate string class
}


//---------------------------------------------------------------------------
//	@function:
//		CMessage::CMessage
//
//	@doc:
//		copy ctor
//
//---------------------------------------------------------------------------
CMessage::CMessage
	(
	const CMessage &msg
	)
	:
	m_ulSev(msg.m_ulSev),
	m_wszFmt(msg.m_wszFmt),
	m_ulLenFmt(msg.m_ulLenFmt),
	m_ulParams(msg.m_ulParams),
	m_wszComment(msg.m_wszComment),
	m_ulLenComment(msg.m_ulLenComment),
	m_exc(msg.m_exc)
{
}


//---------------------------------------------------------------------------
//	@function:
//		CMessageRepository::EresFormat
//
//	@doc:
//		Format error message into given buffer
//
//---------------------------------------------------------------------------
void
CMessage::Format
	(
	CWStringStatic *pwss,
	VA_LIST vl
	)
	const
{
	pwss->AppendFormatVA(m_wszFmt, vl);
}

//---------------------------------------------------------------------------
//	@function:
//		CMessage::FormatMessage
//
//	@doc:
//		Format the message corresponding to the given exception
//
//---------------------------------------------------------------------------
void
CMessage::FormatMessage
	(
	CWStringStatic *pstr,
	ULONG ulMajor,
	ULONG ulMinor,
	...
	)
{
	// manufacture actual exception object
	CException exc(ulMajor, ulMinor);
	
	// during bootstrap there's no context object otherwise, record
	// all details in the context object
	if (NULL != ITask::PtskSelf())
	{
		VA_LIST valist;
		VA_START(valist, ulMinor);

		ELocale eloc = ITask::PtskSelf()->Eloc();
		CMessage *pmsg = CMessageRepository::Pmr()->PmsgLookup(exc, eloc);
		pmsg->Format(pstr, valist);

		VA_END(valist);
	}
}


#ifdef GPOS_DEBUG
//---------------------------------------------------------------------------
//	@function:
//		CMessage::OsPrint
//
//	@doc:
//		debug print of message (without filling in parameters)
//
//---------------------------------------------------------------------------
IOstream &
CMessage::OsPrint
	(
	IOstream &os
	)
{
	os 
		<< "Message No: " << m_exc.UlMajor() << "-" << m_exc.UlMinor() << std::endl
		<< "Message:   \"" << m_wszFmt << "\" [" << m_ulLenFmt << "]" << std::endl
		<< "Parameters: " << m_ulParams << std::endl
		<< "Comments:  \"" << m_wszComment << "\" [" << m_ulLenComment << "]" << std::endl;
	
	return os;
}
#endif // GPOS_DEBUG


//---------------------------------------------------------------------------
//	@function:
//		CMessage::Pmsg
//
//	@doc:
//		Access a message by its index
//
//---------------------------------------------------------------------------
CMessage *
CMessage::Pmsg
	(
	ULONG ulIndex
	)
{
	GPOS_ASSERT(ulIndex < CException::ExmiSentinel);

	// Basic system-side messages in English
	static CMessage rgmsg[CException::ExmiSentinel] =
	{
		CMessage(CException(CException::ExmaInvalid, CException::ExmiInvalid),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Unknown error"),
				 0, // # params
				 GPOS_WSZ_WSZLEN("This message is used if no error message "
								 "can be found at handling time")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiAbort),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Statement aborted"),
				 0, // # params
				 GPOS_WSZ_WSZLEN("Message to indicate statement was" 
								 "canceled (both internal/external)")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiAssert),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("%s:%d: Failed assertion: %ls"),
				 3, // params: filename (CHAR*), line, assertion condition
				 GPOS_WSZ_WSZLEN("Internal assertion has been violated")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiOOM),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Out of memory"),
				 0, // # params
				 GPOS_WSZ_WSZLEN("Memory pool or virtual memory on system exhausted")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiOutOfStack),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Out of stack space"),
				 0, // # params
				 GPOS_WSZ_WSZLEN("Maximally permitted stack allocation exceeded;"
								 "This is not a stack overflow detected by the OS but"
								 "caught internally, i.e., the process is still safe"
								 "to operate afterwards")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiAbortTimeout),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Last check for aborts before %d ms, at:\n%ls"),
				 2, // # params: interval (ULONG), stack trace (WCHAR *)
				 GPOS_WSZ_WSZLEN("Interval between successive abort checkpoints exceeds maximum")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiIOError),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Error during I/O operation, error code: %d"),
				 1, // # params
				 GPOS_WSZ_WSZLEN("I/O operation failed; use error code to identify the error type")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiNetError),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Error during networking operation, error code: %d"),
				 1, // # params
				 GPOS_WSZ_WSZLEN("Networking operation failed; use error code to identify the error type")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiOverflow),
				CException::ExsevError,
				GPOS_WSZ_WSZLEN("Arithmetic Overflow"),
				0, // # params
				GPOS_WSZ_WSZLEN("Arithmetic Overflow")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiInvalidDeletion),
				CException::ExsevError,
				GPOS_WSZ_WSZLEN("Error during delete operation, error code: %d"),
				1, // # params
				GPOS_WSZ_WSZLEN("Delete operation failed; use error code to identify the error type")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiUnexpectedOOMDuringFaultSimulation),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Unexpected out of memory during fault simulation"),
				 0, // # params
				 GPOS_WSZ_WSZLEN("Unexpected out of memory during fault simulation")),

		CMessage(CException(CException::ExmaSystem, CException::ExmiDummyWarning),
				 CException::ExsevWarning,
				 GPOS_WSZ_WSZLEN("This is a dummy warning"),
				 1, // # params
				 GPOS_WSZ_WSZLEN("Used to test warning logging")),

		CMessage(CException(CException::ExmaSQL, CException::ExmiSQLDefault),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Internal error"),
				 0, 
				 GPOS_WSZ_WSZLEN("Internal error")),

		CMessage(CException(CException::ExmaSQL, CException::ExmiSQLNotNullViolation),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Not null constraint for column %ls of table %ls was violated"),
				 2, // column name, table name
				 GPOS_WSZ_WSZLEN("Not null constraint for table was violated")),

		CMessage(CException(CException::ExmaSQL, CException::ExmiSQLCheckConstraintViolation),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Check constraint %ls for table %ls was violated"),
				 2, // constraint name, table name
				 GPOS_WSZ_WSZLEN("Check constraint for table was violated")),

		CMessage(CException(CException::ExmaSQL, CException::ExmiSQLMaxOneRow),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Expected no more than one row to be returned by expression"),
				 0,
				 GPOS_WSZ_WSZLEN("Expected no more than one row to be returned by expression")),

		CMessage(CException(CException::ExmaSQL, CException::ExmiSQLTest),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Test sql error message: %ls"),
				 1, // error message
				 GPOS_WSZ_WSZLEN("Test sql error message")),

		CMessage(CException(CException::ExmaUnhandled, CException::ExmiUnhandled),
				 CException::ExsevError,
				 GPOS_WSZ_WSZLEN("Unhandled exception"),
				 0,
				 GPOS_WSZ_WSZLEN("Unhandled exception")),
	};

	return &rgmsg[ulIndex];
}


// EOF

