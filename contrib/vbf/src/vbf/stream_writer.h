#ifndef VBF_STREAM_WRITER_H
#define VBF_STREAM_WRITER_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <vbf/block_header.h>
#include <vbf/codec.h>
#include <vbf/output_stream.h>

typedef struct stream_writer
{
	output_stream_t       output_stream;
	vbf_codec_t           codec;
	uint8_t              *current_buffer;
	int                   header_length;
	int                   block_size;
	bool                  is_rownum_set;
	int64_t               rownum;
} stream_writer_t;

int stream_writer_init(stream_writer_t *writer,
					   const char *compress_type,
					   int compress_level,
					   int block_size,
					   char *file_name,
					   bool is_create_file,
					   int64_t file_offset);
int stream_writer_flush(stream_writer_t *writer);
void stream_writer_fini(stream_writer_t *writer);
int stream_writer_get_header_length(stream_writer_t *writer);
uint8_t *stream_writer_get_buffer(stream_writer_t *writer);
int stream_writer_append(stream_writer_t *writer,
						 int content_length,
						 int block_kind,
						 int row_count);
int stream_writer_large_append(stream_writer_t *writer,
							   uint8_t *content,
							   int content_length,
							   int block_kind,
							   int row_count);
void stream_writer_set_rownum(stream_writer_t *writer, int64_t rownum);
void stream_writer_reset(stream_writer_t *writer);
int64_t stream_writer_file_size(stream_writer_t *writer);

CLOSE_EXTERN
#endif
