#ifndef VBF_OUTPUT_STREAM_H
#define VBF_OUTPUT_STREAM_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdint.h>
#include <stdbool.h>
#include <vbf/vfs.h>

typedef struct output_stream {
	vbf_file_t         *file;
	uint8_t            *buffer;
	int                 threshold;
	int                 position;
	int64_t             file_offset;
	int64_t             file_length;
} output_stream_t;

int output_stream_init(output_stream_t *stream,
					   bool should_create,
					   char *file_name,
					   int size,
					   int threshold,
					   int64_t offset);
int64_t output_stream_current_position(output_stream_t *stream);
int64_t output_stream_next_position(output_stream_t *stream);
uint8_t *output_stream_get_buffer(output_stream_t *stream);
int output_stream_append(output_stream_t *stream, int used_length);
int64_t output_stream_file_size(output_stream_t *stream);
int output_stream_flush(output_stream_t *stream);
void output_stream_fini(output_stream_t *stream);

CLOSE_EXTERN
#endif
