#include <stdlib.h>
#include <string.h>

#include "vbf_private.h"
#include "vbf/allocation.h"

/* C89 allows these to be macros */
#undef malloc
#undef free

vbf_allocator_state_t VBF_CURRENT_ALLOCATOR = {
	malloc,
	free
};

void vbf_register_allocator(vbf_malloc_t malloc_fn, vbf_free_t free_fn)
{
	VBF_CURRENT_ALLOCATOR.malloc_fn = malloc_fn;
	VBF_CURRENT_ALLOCATOR.free_fn = free_fn; 
}

void *vbf_calloc(size_t count, size_t size)
{
	void *ptr = vbf_malloc(count * size);

	if (ptr != NULL) {
		memset(ptr, 0, count * size);
	}
	return ptr;
}

char *vbf_strdup(const char *str)
{
	size_t  str_size;
	char   *new_str;

	if (str == NULL) {
		return NULL;
	}

	str_size = strlen(str) + 1;
	new_str = vbf_malloc(str_size);
	memcpy(new_str, str, str_size);

	return new_str;
}

char *vbf_strndup(const char *str, size_t size)
{
	char *new_str;

	if (str == NULL) {
		return NULL;
	}

	new_str = vbf_malloc(size + 1);
	memcpy(new_str, str, size);
	new_str[size] = '\0';

	return new_str;
}
