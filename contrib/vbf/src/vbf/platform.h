#ifndef VBF_PLATFORM_H
#define VBF_PLATFORM_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

/* Use this header file to include platform specific definitions */
#include <inttypes.h>

/* Defines for printing size_t. */
#define PRIsz "zu"

CLOSE_EXTERN
#endif
