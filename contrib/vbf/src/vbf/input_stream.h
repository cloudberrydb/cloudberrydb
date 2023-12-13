#ifndef VBF_INPUT_STREAM_H
#define VBF_INPUT_STREAM_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdint.h>
#include <stdbool.h>
#include <vbf/vfs.h>

typedef struct input_stream {
	vbf_file_t *file;
	int         block_size;
	int         threshold;
	uint8_t    *memory;
	uint8_t    *buffer;
	int64_t     read_position;
	int         read_length;
	int         buffer_offset;
	int         buffer_length;
	int64_t     file_length;
	off_t       file_offset;
} input_stream_t;

int input_stream_init(input_stream_t *stream,
					  int block_size,
					  int threshold);
void input_stream_fini(input_stream_t *stream);
int input_stream_reset(input_stream_t *stream,
					   char *file_name,
					   int64_t file_length);
int64_t input_stream_next_buffer_position(input_stream_t *stream);
int input_stream_get_next_buffer(input_stream_t *stream,
								 int request_length,
								 int *available_length,
								 uint8_t **buffer);
int input_stream_grow_buffer(input_stream_t *stream,
							 int request_length,
							 int *available_length,
							 uint8_t **buffer);
uint8_t *input_stream_get_current_buffer(input_stream_t *stream);
int64_t input_stream_current_position(input_stream_t *stream);

CLOSE_EXTERN
#endif
