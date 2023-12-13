#ifndef VBF_VFS_H
#define VBF_VFS_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

typedef struct vbf_file vbf_file_t;
struct vbf_file {
	struct vbf_io_methods *methods;  /* Methods for an open file */
};

typedef struct vbf_io_methods vbf_io_methods_t;
struct vbf_io_methods {
	void (*close) (vbf_file_t *file);
	int (*read) (vbf_file_t *file, char *buffer, int amount, off_t offset);
	int (*write) (vbf_file_t *file, char *buffer, int amount, off_t offset);
	off_t (*seek) (vbf_file_t *file, off_t offset, int whence);
	int (*sync) (vbf_file_t *file);
	int64_t (*size) (vbf_file_t *file);
	int (*truncate) (vbf_file_t *file, int64_t size);
	const char *(*name) (vbf_file_t *file);
};

typedef struct vbf_vfs vbf_vfs_t;
struct vbf_vfs {
	const char *name;
	vbf_file_t *(*open) (const char *file_name, int flags);
	int (*remove) (const char *file_name);
	int (*get_create_flags) (void);
	int (*get_open_flags) (bool);
	vbf_io_methods_t io_methods;
};

void vbf_register_vfs(vbf_vfs_t *vfs);

CLOSE_EXTERN
#endif
