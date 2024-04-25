#pragma once
#include "comm/cbdb_api.h"

#include <sstream>
#include <string>

// CBDB_TRY();
// {
//   // C++ implementation code
// }
// CBDB_CATCH_MATCH(std::exception &exp); // optional
// {
//    // specific exception handler
//    error_message.Append("error message: %s", error_message.Message());
// }
// CBDB_CATCH_DEFAULT();
// CBDB_END_TRY();
//
// CBDB_CATCH_MATCH() is optional and can have several match pattern.


// being of a try block w/o explicit handler
#define CBDB_TRY()                                          \
  do {                                                      \
    bool internal_cbdb_try_throw_error_ = false;            \
    bool internal_cbdb_try_throw_error_with_stack_ = false; \
    cbdb::ErrorMessage error_message;                       \
    try {
// begin of a catch block
#define CBDB_CATCH_MATCH(exception_decl) \
  }                                      \
  catch (exception_decl) {               \
    internal_cbdb_try_throw_error_ = true;

// catch c++ exception and rethrow ERROR to C code
// only used by the outer c++ code called by C
#define CBDB_CATCH_DEFAULT()                          \
  }                                                   \
  catch (cbdb::CException & e) {                      \
    internal_cbdb_try_throw_error_ = true;            \
    internal_cbdb_try_throw_error_with_stack_ = true; \
    global_pg_error_message = elog_message();         \
    elog(LOG, "\npax stack trace: \n%s", e.Stack());  \
    global_exception = e;                             \
  }                                                   \
  catch (...) {                                       \
    internal_cbdb_try_throw_error_ = true;            \
    internal_cbdb_try_throw_error_with_stack_ = false;

// like PG_FINALLY
#define CBDB_FINALLY(...) \
  }                       \
  {                       \
    do {                  \
      __VA_ARGS__;        \
    } while (0);

// end of a try-catch block
#define CBDB_END_TRY()                                                      \
  }                                                                         \
  if (internal_cbdb_try_throw_error_) {                                     \
    if (global_pg_error_message) {                                          \
      elog(LOG, "\npg error message:%s", global_pg_error_message);          \
    }                                                                       \
    if (internal_cbdb_try_throw_error_with_stack_) {                        \
      elog(LOG, "\npax stack trace: \n%s", global_exception.Stack());       \
      ereport(                                                              \
          ERROR,                                                            \
          errmsg("%s (PG message: %s)", global_exception.What().c_str(),    \
                 !global_pg_error_message ? "" : global_pg_error_message)); \
    }                                                                       \
    if (error_message.Length() == 0)                                        \
      error_message.Append("ERROR: %s", __func__);                          \
    ereport(ERROR, errmsg("%s", error_message.Message()));                  \
  }                                                                         \
  }                                                                         \
  while (0)

namespace cbdb {

#define DEFAULT_STACK_MAX_DEPTH 63
#define DEFAULT_STACK_MAX_SIZE \
  ((DEFAULT_STACK_MAX_DEPTH + 1) * PIPE_MAX_PAYLOAD)
// error message buffer
class ErrorMessage final {
 public:
  ErrorMessage();
  ErrorMessage(const ErrorMessage &message);
  void Append(const char *format, ...) noexcept;
  void AppendV(const char *format, va_list ap) noexcept;
  const char *Message() const noexcept;
  int Length() const noexcept;

 private:
  int index_ = 0;
  char message_[128];
};

class CException {
 public:
  enum ExType {
    kExTypeInvalid = 0,
    kExTypeUnImplements,
    kExTypeAssert,
    kExTypeAbort,
    kExTypeOOM,
    kExTypeIOError,
    kExTypeCError,
    kExTypeLogicError,
    kExTypeInvalidMemoryOperation,
    kExTypeSchemaNotMatch,
    kExTypeInvalidPORCFormat,
    kExTypeOutOfRange,
    kExTypeFileOperationError,
    kExTypeCompressError,
    kExTypeArrowExportError,
    kExTypeArrowBatchSizeTooSmall,
  };

  explicit CException(ExType extype);

  CException(const char *filename, int lineno, ExType extype);

  const char *Filename() const;

  int Lineno() const;

  ExType EType() const;

  std::string What() const;

  const char *Stack() const;

  static void Raise(const char *filename, int line, ExType extype)
      __attribute__((__noreturn__));
  static void Raise(CException ex, bool reraise) __attribute__((__noreturn__));
  static void ReRaise(CException ex) __attribute__((__noreturn__));

 private:
  char stack_[DEFAULT_STACK_MAX_SIZE];
  static const char *exception_names[];
  const char *m_filename_;
  int m_lineno_;
  ExType m_extype_;
};

}  // namespace cbdb

extern cbdb::CException global_exception;
extern char *global_pg_error_message;

#define CBDB_RAISE(...) cbdb::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)
#define CBDB_RERAISE(ex) cbdb::CException::ReRaise(ex)
#define CBDB_CHECK(check, ...) \
  do {                         \
    if (unlikely(!(check))) {  \
      CBDB_RAISE(__VA_ARGS__); \
    }                          \
  } while (0)
