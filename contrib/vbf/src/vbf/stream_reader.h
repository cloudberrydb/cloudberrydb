#ifndef VBF_STREAM_READER_H
#define VBF_STREAM_READER_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <vbf/block_header.h>
#include <vbf/codec.h>
#include <vbf/input_stream.h>

typedef struct header_info
{
	int64_t       offset;
	header_kind_t kind;
	int           length;
	int           block_length;
	int           content_offset;
	int           uncompressed_length;
	int           block_kind;
	bool          has_rownum;
	int64_t       rownum;
	int           row_count;
	bool          is_large;
	bool          is_compressed;
	int           compressed_length;
} header_info_t;

typedef struct stream_reader
{
	input_stream_t input_stream;
	vbf_codec_t    codec;
	header_info_t  header;
} stream_reader_t;

int stream_reader_init(stream_reader_t *reader,
					  const char *compress_type,
					  int compress_level,
					  int block_size);
void stream_reader_fini(stream_reader_t *reader);
int stream_reader_reset(stream_reader_t *reader,
						char *file_name,
						int64_t file_length);
int stream_reader_get_buffer(stream_reader_t *reader, uint8_t **content);
int stream_reader_get_content(stream_reader_t *reader,
							  uint8_t *content_out,
							  int content_out_length);
int stream_reader_get_block_info(stream_reader_t *reader,
								 bool *has_next,
								 int *content_length,
								 int *block_kind,
								 int64_t *rownum,
								 int *row_count,
								 bool *is_large,
								 bool *is_compressed);
int64_t stream_reader_get_compressed_length(stream_reader_t *reader);

CLOSE_EXTERN
#endif
