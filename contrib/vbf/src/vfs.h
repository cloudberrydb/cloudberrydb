#ifndef VBF_VFS_PRIVATE_H
#define VBF_VFS_PRIVATE_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include "vbf/vfs.h"

vbf_file_t *vbf_file_open(const char *file_name, int flags);
int vbf_file_delete(const char *file_name);
void vbf_file_close(vbf_file_t *file);
int vbf_file_read(vbf_file_t *file, char *buffer, int amount, off_t offset);
int vbf_file_write(vbf_file_t *file, char *buffer, int amount, off_t offset);
off_t vbf_file_seek(vbf_file_t *file, off_t offset, int whence);
int vbf_file_sync(vbf_file_t *file);
off_t vbf_file_size(vbf_file_t *file);
int vbf_file_truncate(vbf_file_t *file, int64_t size);
const char *vbf_file_name(vbf_file_t *file);
int vbf_file_get_create_flags(void);
int vbf_file_get_open_flags(bool is_readable);

CLOSE_EXTERN
#endif
