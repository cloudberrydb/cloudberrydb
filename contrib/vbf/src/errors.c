#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <pthread.h>

#include "vbf/errors.h"

/* 4K should be enough, right? */
#define VBF_ERROR_SIZE 4096


/*
 * To support the vbf_prefix_error function, we keep two string buffers
 * around.  The VBF_CURRENT_ERROR points at the buffer that's holding
 * the current error message.  vbf_prefix error writes into the other
 * buffer, and then swaps them.
 */

struct vbf_error_data_t {
    char  VBF_ERROR1[VBF_ERROR_SIZE];
    char  VBF_ERROR2[VBF_ERROR_SIZE];

    char  *VBF_CURRENT_ERROR;
    char  *VBF_OTHER_ERROR;
};

static pthread_key_t error_data_key;
static pthread_once_t error_data_key_once = PTHREAD_ONCE_INIT;

static void make_error_data_key()
{
    pthread_key_create(&error_data_key, free);
}

static struct vbf_error_data_t *vbf_get_error_buffer(void)
{
	struct vbf_error_data_t *ERROR_DATA;

	pthread_once(&error_data_key_once, make_error_data_key);

	ERROR_DATA = (struct vbf_error_data_t*) pthread_getspecific(error_data_key);
	if (!ERROR_DATA) {
        ERROR_DATA = (struct vbf_error_data_t*) malloc(sizeof(struct vbf_error_data_t));
        pthread_setspecific(error_data_key, ERROR_DATA);

        ERROR_DATA->VBF_ERROR1[0] = '\0';
        ERROR_DATA->VBF_ERROR2[0] = '\0';
        ERROR_DATA->VBF_CURRENT_ERROR = ERROR_DATA->VBF_ERROR1;
        ERROR_DATA->VBF_OTHER_ERROR = ERROR_DATA->VBF_ERROR2;
    }

    return ERROR_DATA;
}

void vbf_set_error(const char *fmt, ...)
{
	va_list  args;

	va_start(args, fmt);
	vsnprintf(vbf_get_error_buffer()->VBF_CURRENT_ERROR, VBF_ERROR_SIZE, fmt, args);
	va_end(args);
}

const char *vbf_strerror(void)
{
	return vbf_get_error_buffer()->VBF_CURRENT_ERROR;
}

void vbf_prefix_error(const char *fmt, ...)
{
	char *tmp;
	struct vbf_error_data_t *ERROR_DATA = vbf_get_error_buffer();

	/*
	 * First render the prefix into OTHER_ERROR.
	 */
	va_list  args;
	va_start(args, fmt);
	int  bytes_written = vsnprintf(ERROR_DATA->VBF_OTHER_ERROR, VBF_ERROR_SIZE, fmt, args);
	va_end(args);

	/*
	 * Then concatenate the existing error onto the end.
	 */
	if (bytes_written < VBF_ERROR_SIZE) {
		strncpy(&ERROR_DATA->VBF_OTHER_ERROR[bytes_written], ERROR_DATA->VBF_CURRENT_ERROR,
			VBF_ERROR_SIZE - bytes_written);
		ERROR_DATA->VBF_OTHER_ERROR[VBF_ERROR_SIZE - 1] = '\0';
	}

	/*
	 * Swap the two error pointers.
	 */
	tmp = ERROR_DATA->VBF_OTHER_ERROR;
	ERROR_DATA->VBF_OTHER_ERROR = ERROR_DATA->VBF_CURRENT_ERROR;
	ERROR_DATA->VBF_CURRENT_ERROR = tmp;
}

const char *posix_get_last_error(void)
{
	strerror_r(errno, vbf_get_error_buffer()->VBF_OTHER_ERROR, VBF_ERROR_SIZE);
	return vbf_get_error_buffer()->VBF_OTHER_ERROR;
}
