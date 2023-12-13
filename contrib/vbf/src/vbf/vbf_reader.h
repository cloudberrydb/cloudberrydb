#ifndef VBF_READER_H
#define VBF_READER_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <vbf/block_layout.h>
#include <vbf/stream_reader.h>

typedef struct vbf_reader
{
	block_reader_t  block_reader;
	stream_reader_t stream_reader;
	int64_t         rownum;
	int             data_length;
	int             block_kind;
	int             row_count;
	bool            is_large;
	bool            is_compressed;
	uint8_t        *uncompressed_buffer;
	uint8_t        *large_buffer;
	int             large_buffer_length;
	int             item_count;
	int             current_item_count;
	uint8_t        *single_buffer;
	bool            need_next_block;
} vbf_reader_t;

int vbf_reader_init(vbf_reader_t *reader,
					const char *compress_type,
					int compress_level,
					int block_size);
void vbf_reader_fini(vbf_reader_t *reader);
int vbf_reader_reset(vbf_reader_t *reader,
					 char *file_name,
					 int64_t file_length);
int vbf_reader_next(vbf_reader_t *reader,
					 uint8_t **buffer,
					 int *buffer_length,
					 bool *has_next);
int vbf_reader_next_with_rownum(vbf_reader_t *reader,
								uint8_t **buffer,
								int *buffer_length,
								int64_t *rownum,
								bool *has_next);

CLOSE_EXTERN
#endif
