#pragma once
#include <cstdarg>
#include <cstring>
#include <memory>
#include <string>

namespace pax {

#define MAX_LEN_OF_FMT_STR 2048
static char format_str[MAX_LEN_OF_FMT_STR] = {0};

// Do not use the (Args...) to forward args
// need use the __format__ to prevent potential type problems
static inline __attribute__((__format__(__printf__, 1, 2))) std::string fmt(
    const char *format, ...) {
  va_list args;
  va_start(args, format);
  vsnprintf(format_str, MAX_LEN_OF_FMT_STR - 1, format, args);
  va_end(args);

  return std::string(format_str);
}

}  // namespace pax