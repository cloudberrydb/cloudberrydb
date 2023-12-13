#ifndef VBF_ERRORS_H
#define VBF_ERRORS_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

/*
 * Returns a textual description of the last error condition returned by
 * an Vbf function.
 */
const char *vbf_strerror(void);
void vbf_set_error(const char *fmt, ...);
void vbf_prefix_error(const char *fmt, ...);

const char *posix_get_last_error(void);

CLOSE_EXTERN
#endif
