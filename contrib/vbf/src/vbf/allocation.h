#ifndef VBF_ALLOCATION_H
#define VBF_ALLOCATION_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdlib.h>

typedef void *(*vbf_malloc_t) (size_t);
typedef void (*vbf_free_t) (void *);

void vbf_register_allocator(vbf_malloc_t malloc_fn, vbf_free_t free_fn);

typedef struct vbf_allocator_state {
	vbf_malloc_t malloc_fn;
	vbf_free_t free_fn;
} vbf_allocator_state_t;

extern vbf_allocator_state_t VBF_CURRENT_ALLOCATOR;

#define vbf_malloc(sz) (VBF_CURRENT_ALLOCATOR.malloc_fn((sz)))
#define vbf_free(ptr) (VBF_CURRENT_ALLOCATOR.free_fn((ptr)))
void *vbf_calloc(size_t count, size_t size);

char *vbf_strdup(const char *str);
char *vbf_strndup(const char *str, size_t size);

CLOSE_EXTERN
#endif
