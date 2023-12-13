#ifndef VBF_TEST_H
#define VBF_TEST_H

#include <stdio.h>

#define CHECK_SUCCESS(name, ret, call) \
	{ \
		ret = call; \
		if (ret == -1) { \
			fprintf(stderr, "failed to call %s(): %s\n", name, vbf_strerror()); \
			exit(EXIT_FAILURE); \
		} \
	}

#define CHECK_NOT_NULL(name, ret, call) \
	{ \
		ret = call; \
		if (ret == NULL) { \
			fprintf(stderr, "failed to call %s(): %s\n", name, vbf_strerror()); \
			exit(EXIT_FAILURE); \
		} \
	}

#endif
