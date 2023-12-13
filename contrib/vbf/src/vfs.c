#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>

#include "vbf/vfs.h"
#include "vbf/errors.h"

#define MODE_OWNER (S_IRUSR | S_IWUSR)

static vbf_file_t *posix_file_open(const char *file_name, int flags);
static int posix_file_delete(const char *file_name);
static int posix_get_create_flags(void);
static int posix_get_open_flags(bool);
static void posix_file_close(vbf_file_t *file);
static int posix_file_read(vbf_file_t *file, char *buffer, int amount, off_t offset);
static int posix_file_write(vbf_file_t *file, char *buffer, int amount, off_t offset);
static off_t posix_file_seek(vbf_file_t *file, off_t offset, int whence);
static int posix_file_sync(vbf_file_t *file);
static int64_t posix_file_size(vbf_file_t *file);
static int posix_file_truncate(vbf_file_t *file, int64_t size);
static const char *posix_file_name(vbf_file_t *file);

vbf_vfs_t VBF_CURRENT_VFS = {
	"posix",
	posix_file_open,
	posix_file_delete,
	posix_get_create_flags,
	posix_get_open_flags,
	{
		posix_file_close,
		posix_file_read,
		posix_file_write,
		posix_file_seek,
		posix_file_sync,
		posix_file_size,
		posix_file_truncate,
		posix_file_name
	}
};

typedef struct posix_file {
	vbf_file_t  base;
	int         fd;
	char       *file_name;
} posix_file_t;

static vbf_file_t *posix_file_open(const char *file_name, int flags)
{
	int length;
	char *new_str = NULL;
	posix_file_t *file = NULL;

	length = strlen(file_name) + 1;
	new_str = malloc(length);
	if (new_str == NULL) {
		vbf_set_error("failed to open file \"%s\": out of memory", file_name);
		goto error;
	}
	memcpy(new_str, file_name, length);

	file = malloc(sizeof(posix_file_t));
	if (file == NULL) {
		vbf_set_error("failed to open file \"%s\": out of memory", file_name);
		goto error;
	}

	file->fd = open(file_name, flags, MODE_OWNER);
	if (file->fd < 0) {
		vbf_set_error("failed to open file \"%s\": %s", file_name, posix_get_last_error());
		goto error;
	}

	file->file_name = new_str;

	return (vbf_file_t *) file;

error:
	if (new_str != NULL) {
		free(new_str);
	}

	if (file != NULL) {
		free(file);
	}
	return NULL;
}

static int posix_file_delete(const char *file_name)
{
	if (unlink(file_name) == -1) {
		vbf_set_error("failed to unlink file %s: %s", file_name, posix_get_last_error());
		return -1;
	}

	return 0;
}

static int posix_get_create_flags(void)
{
	return O_RDWR | O_CREAT | O_EXCL;
}

static int posix_get_open_flags(bool is_readable)
{
	return is_readable ? O_RDONLY : O_RDWR;
}

static void posix_file_close(vbf_file_t *file)
{
	posix_file_t *posix_file = (posix_file_t *) file;

	close(posix_file->fd);
	free(posix_file->file_name);
	free(posix_file);
}

static int posix_file_read(vbf_file_t *file, char *buffer, int amount, off_t offset)
{
	int result;
	posix_file_t *posix_file = (posix_file_t *) file;

	if (lseek(posix_file->fd, offset, SEEK_SET) < 0) {
		vbf_set_error("failed to seek file %s: %s", posix_file->file_name, posix_get_last_error());
		return -1;
	}

	result = read(posix_file->fd, buffer, amount);
	if (result == -1) {
		vbf_set_error("failed to read file %s: %s", posix_file->file_name, posix_get_last_error());
		return -1;
	}

	return result;
}

static int posix_file_write(vbf_file_t *file, char *buffer, int amount, off_t offset)
{
	int result;
	posix_file_t *posix_file = (posix_file_t *) file;

	if (lseek(posix_file->fd, offset, SEEK_SET) < 0) {
		vbf_set_error("failed to seek file %s: %s", posix_file->file_name, posix_get_last_error());
		return -1;
	}

	result = write(posix_file->fd, buffer, amount);
	if (result == -1) {
		vbf_set_error("failed to write file %s: %s", posix_file->file_name, posix_get_last_error());
		return -1;
	}

	return result;
}

static off_t posix_file_seek(vbf_file_t *file, off_t offset, int whence)
{
	posix_file_t *posix_file = (posix_file_t *) file;

	if (lseek(posix_file->fd, offset, whence) == -1) {
		vbf_set_error("failed to seek file %s: %s", posix_file->file_name, posix_get_last_error());
		return -1;
	}

	return 0;
}

static int posix_file_sync(vbf_file_t *file)
{
	posix_file_t *posix_file = (posix_file_t *) file;

	if (fsync(posix_file->fd) == -1) {
		vbf_set_error("failed to fsync file %s: %s", posix_file->file_name, posix_get_last_error());
		return -1;
	}

	return 0;
}

static int64_t posix_file_size(vbf_file_t *file)
{
	int	returnCode;
	struct stat buf;
	posix_file_t *posix_file = (posix_file_t *) file;

	returnCode = fstat(posix_file->fd, &buf);
	if (returnCode < 0) {
		vbf_set_error("failed to fstat file %s: %s", posix_file->file_name, posix_get_last_error());
		return returnCode;
	}

	return (int64_t) buf.st_size;
}

static int posix_file_truncate(vbf_file_t *file, int64_t size)
{
	posix_file_t *posix_file = (posix_file_t *) file;

	if (ftruncate(posix_file->fd, size)) {
		vbf_set_error("failed to ftruncate file %s: %s", posix_file->file_name, posix_get_last_error());
		return -1;
	}

	return 0;
}

static const char *posix_file_name(vbf_file_t *file)
{
	posix_file_t *posix_file = (posix_file_t *) file;
	return posix_file->file_name;
}

void vbf_register_vfs(vbf_vfs_t *vfs)
{
	VBF_CURRENT_VFS = *vfs;
}

vbf_file_t *vbf_file_open(const char *file_name, int flags)
{
	vbf_file_t *file;

	file = VBF_CURRENT_VFS.open(file_name, flags);
	if (file == NULL) {
		return NULL;
	}

	file->methods = &VBF_CURRENT_VFS.io_methods;

	return file;
}

int vbf_file_delete(const char *file_name)
{
	return VBF_CURRENT_VFS.remove(file_name);
}

void vbf_file_close(vbf_file_t *file)
{
	if (file->methods) {
		file->methods->close(file);
		file->methods = NULL;
	}
}

int vbf_file_read(vbf_file_t *file, char *buffer, int amount, off_t offset)
{
	return file->methods->read(file, buffer, amount, offset);
}

int vbf_file_write(vbf_file_t *file, char *buffer, int amount, off_t offset)
{
	return file->methods->write(file, buffer, amount, offset);
}

off_t vbf_file_seek(vbf_file_t *file, off_t offset, int whence)
{
	return file->methods->seek(file, offset, whence);
}

int vbf_file_sync(vbf_file_t *file)
{
	return file->methods->sync(file);
}

off_t vbf_file_size(vbf_file_t *file)
{
	return file->methods->size(file);
}

int vbf_file_truncate(vbf_file_t *file, int64_t size)
{
	return file->methods->truncate(file, size);
}

const char *vbf_file_name(vbf_file_t *file)
{
	return file->methods->name(file);
}

int vbf_file_get_create_flags(void)
{
	return VBF_CURRENT_VFS.get_create_flags();
}

int vbf_file_get_open_flags(bool is_readable)
{
	return VBF_CURRENT_VFS.get_open_flags(is_readable);
}
