//---------------------------------------------------------------------------
//	Greenplum Database
//	Copyright (C) 2008 Greenplum, Inc.
//
//	@filename:
//		CException.h
//
//	@doc:
//		Implements basic exception handling. All excpetion handling related
//		functionality is wrapped in macros to facilitate easy modifications
//		at a later point in time.
//---------------------------------------------------------------------------
#ifndef GPOS_CException_H
#define GPOS_CException_H

#include "gpos/types.h"

// SQL state code length
#define GPOS_SQLSTATE_LENGTH 5

// standard way to raise an exception
#define GPOS_RAISE(...) \
	gpos::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)

// raises GPOS exception,
// these exceptions can later be translated to GPDB log severity levels
// so that they can be written in GPDB with appropriate severity level.
#define GPOS_THROW_EXCEPTION(...) \
	gpos::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)

// helper to match a caught exception
#define GPOS_MATCH_EX(ex, ulMajor, ulMinor) \
									(ulMajor == ex.UlMajor() && ulMinor == ex.UlMinor())

// being of a try block w/o explicit handler
#define GPOS_TRY \
									do \
									{ \
										CErrorHandler *perrhdl__ = NULL; \
										try \
										{

// begin of a try block
#define GPOS_TRY_HDL(perrhdl) \
									do \
									{ \
										CErrorHandler *perrhdl__ = perrhdl; \
										try \
										{

// begin of a catch block
#define GPOS_CATCH_EX(exc) \
										} \
										catch(gpos::CException &exc) \
										{ \
											{ if (NULL != perrhdl__) perrhdl__->Process(exc); }
											

// end of a catch block
#define GPOS_CATCH_END \
										} \
									} while (0)

// to be used inside a catch block
#define GPOS_RESET_EX				ITask::PtskSelf()->Perrctxt()->Reset()
#define GPOS_RETHROW(exc)			gpos::CException::Reraise(exc)
	
// short hands for frequently used exceptions
#define GPOS_ABORT					GPOS_RAISE(CException::ExmaSystem, CException::ExmiAbort)
#define GPOS_OOM_CHECK(x)			do \
									{ \
										if (NULL == (void*)x) \
											{ \
											GPOS_RAISE(CException::ExmaSystem, CException::ExmiOOM); \
											} \
									} \
									while (0)

namespace gpos
{

	//---------------------------------------------------------------------------
	//	@class:
	//		CException
	//
	//	@doc:
	//		Basic exception class -- used for "throw by value/catch by reference"
	//		Contains only a category (= major) and a error (= minor).
	//
	//---------------------------------------------------------------------------
	class CException
	{
		public:
			// majors - reserve range 0-99
			enum ExMajor
			{
				ExmaInvalid = 0,
				ExmaSystem	= 1,
	
				ExmaSQL = 2,
				
				ExmaUnhandled = 3,

				ExmaSentinel
			};
	
			// minors
			enum ExMinor
			{
				// system errors
				ExmiInvalid = 0,
				ExmiAbort,
				ExmiAssert,
				ExmiOOM,
				ExmiOutOfStack,
				ExmiAbortTimeout,
				ExmiIOError,
				ExmiNetError,
				ExmiOverflow,
				ExmiInvalidDeletion,
	
				// unexpected OOM during fault simulation 
				ExmiUnexpectedOOMDuringFaultSimulation,
				
				// sql exceptions
				ExmiSQLDefault,
				ExmiSQLNotNullViolation,
				ExmiSQLCheckConstraintViolation,
				ExmiSQLMaxOneRow,
				ExmiSQLTest,
				
				// warnings
				ExmiDummyWarning,

				// unknown exception
				ExmiUnhandled,

				// illegal byte sequence
				ExmiIllegalByteSequence,

				ExmiSentinel
			};
		
		// structure for mapping exceptions to SQLerror codes
		private:
			struct SErrCodeElem
			{
				// exception number
				ULONG m_ul;
				
				// SQL standard error code
				const CHAR *m_szSQLState;
			};
			
			// error range
			ULONG m_ulMajor;
			
			// error number
			ULONG m_ulMinor;
			
			// SQL state error code
			const CHAR *m_szSQLState;
			
			// filename
			CHAR *m_szFilename;
			
			// line in file
			ULONG m_ulLine;

			// severity level mapped to GPDB log severity level
			ULONG m_ulSeverityLevel;

			// sql state error codes
			static
			const SErrCodeElem m_rgerrcode[ExmiSQLTest - ExmiSQLDefault + 1];


			// internal raise API
			static
			void Raise(CException exc) __attribute__((__noreturn__));

			// get sql error code for given exception
			static
			const CHAR *SzSQLState(ULONG ulMajor, ULONG ulMinor);
			
		public:

			// severity levels
			enum ExSeverity
			{
				ExsevInvalid = 0,
				ExsevPanic,
				ExsevFatal,
				ExsevError,
				ExsevWarning,
				ExsevNotice,
				ExsevTrace,
				ExsevDebug1,

				ExsevSentinel
			};

			// severity levels
			static
			const CHAR *m_rgszSeverity[ExsevSentinel];

			// ctor
			CException(ULONG ulMajor, ULONG ulMinor);
			CException(ULONG ulMajor, ULONG ulMinor, const CHAR *szFilename, ULONG ulLine);
			CException(ULONG ulMajor, ULONG ulMinor, const CHAR *szFilename, ULONG ulLine, ULONG ulSeverityLevel);

			// accessors
			ULONG UlMajor() const
			{
				return m_ulMajor;
			}
		
			ULONG UlMinor() const
			{
				return m_ulMinor;
			}

			const CHAR *SzFilename() const
			{
				return m_szFilename;
			}
			
			ULONG UlLine() const
			{
				return m_ulLine;
			}

			ULONG UlSeverityLevel() const
			{
				return m_ulSeverityLevel;
			}

			const CHAR *SzSQLState() const
			{
				return m_szSQLState;
			}
			
			// simple equality
			BOOL operator == 
				(
				const CException &exc
				)
				const
			{
				return m_ulMajor == exc.m_ulMajor && m_ulMinor == exc.m_ulMinor;
			}

			
			// simple inequality
			BOOL operator !=
				(
				const CException &exc
				)
				const
			{
				return !(*this == exc);
			}

			// equality function -- needed for hashtable
			static
			BOOL FEqual
				(
				const CException &exc,
				const CException &excOther
				)
			{
				return exc == excOther;
			}

			// basic hash function
			static
			ULONG UlHash
				(
				const CException &exc
				)
			{
				return exc.m_ulMajor ^ exc.m_ulMinor;
			}

			// wrapper around throw
			static 
			void Raise(const CHAR *szFilename, ULONG ulLine, ULONG ulMajor, ULONG ulMinor,...) __attribute__((__noreturn__));

			// wrapper around throw with severity level
			static
			void Raise(const CHAR *szFilename, ULONG ulLine, ULONG ulMajor, ULONG ulMinor, ULONG ulSeverityLevel, ...) __attribute__((__noreturn__));

			// rethrow wrapper
			static
			void Reraise(CException exc, BOOL fPropagate = false) __attribute__((__noreturn__));

			// invalid exception
			static
			const CException m_excInvalid;
			
	}; // class CException
}

#endif // !GPOS_CException_H

// EOF

