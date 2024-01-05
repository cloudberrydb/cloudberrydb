#ifndef VBF_PRIVATE_H
#define VBF_PRIVATE_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <errno.h>

#include "vbf/errors.h"
#include "vbf/platform.h"

#define storage_round_up4(l) ((((l) + 3) / 4) * 4) /* 32-bit alignment */
#define storage_round_up8(l) ((((l) + 7) / 8) * 8) /* 64-bit alignment */
#define storage_round_up(l) (storage_round_up8(l))
#define	storage_zero_pad(\
			buf,\
			len,\
			padToLen)\
{\
	int b;\
\
	for (b = len; b < padToLen; b++)\
		buf[b] = 0;\
}

#define check(rval, call) { rval = call; if (rval) return rval; }

#define check_set(rval, call, ...)			\
	{						\
		rval = call;				\
		if (rval) {				\
			vbf_set_error(__VA_ARGS__);	\
			return rval;			\
		}					\
	}

#define check_prefix(rval, call, ...)			\
	{						\
		rval = call;				\
		if (rval) {				\
			vbf_prefix_error(__VA_ARGS__);	\
			return rval;			\
		}					\
	}

#define check_param(result, test, name)					\
	{								\
		if (!(test)) {						\
			vbf_set_error("Invalid " name " in %s",	\
				       __FUNCTION__);			\
			return result;					\
		}							\
	}

#define VBF_UNUSED(var) (void) var;

#define container_of(ptr_, type_, member_)  \
    ((type_ *) ((char *) ptr_ - (size_t) &((type_ *) 0)->member_))

#define nullstrcmp(s1, s2) \
    (((s1) && (s2)) ? strcmp(s1, s2) : ((s1) || (s2)))

#define lengthof(array) (sizeof (array) / sizeof ((array)[0])) 

CLOSE_EXTERN
#endif
