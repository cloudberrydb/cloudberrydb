#pragma once

// Should never call PAX_LOG* without PAX_ENABLE_DEBUG
#define PAX_LOG_IF(ok, ...)         \
  do {                              \
    if (ok) elog(LOG, __VA_ARGS__); \
  } while (0)

#define PAX_LOG(...)        \
  do {                      \
    elog(LOG, __VA_ARGS__); \
  } while (0)
