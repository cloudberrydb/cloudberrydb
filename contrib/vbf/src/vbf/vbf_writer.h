#ifndef VBF_WRITER_H
#define VBF_WRITER_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <vbf/block_layout.h>
#include <vbf/stream_writer.h>

typedef struct vbf_writer
{
	block_maker_t        block_maker;
	stream_writer_t      stream_writer;
	uint8_t             *buffer;
	int64_t              rownum;
} vbf_writer_t;

int vbf_writer_init(vbf_writer_t *writer,
					const char *compress_type,
					int compress_level,
					int block_size,
					char *file_name,
					bool is_create_file,
					int64_t file_offset,
					int64_t rownum);
int vbf_writer_flush(vbf_writer_t *writer);
void vbf_writer_fini(vbf_writer_t *writer);
int vbf_writer_write(vbf_writer_t *writer,
					 uint8_t *buffer,
					 int buffer_length);
int64_t vbf_writer_file_size(vbf_writer_t *writer);

CLOSE_EXTERN
#endif
