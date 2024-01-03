#pragma once
#include "comm/cbdb_api.h"

#include <sstream>
#include <string>

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
    kExTypeInvalidORCFormat,
    kExTypeOutOfRange,
    kExTypeFileOperationError,
    kExTypeCompressError,
    kExTypeArrowExportError,
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

#define CBDB_RAISE(...) cbdb::CException::Raise(__FILE__, __LINE__, __VA_ARGS__)
#define CBDB_RERAISE(ex) cbdb::CException::ReRaise(ex)
#define CBDB_CHECK(check, ...) \
  do {                         \
    if (unlikely(!(check))) {  \
      CBDB_RAISE(__VA_ARGS__); \
    }                          \
  } while (0)
