#include <string.h>
#include <stdlib.h>

#include "vbf/allocation.h"
#include "vbf/errors.h"
#include "vbf/output_stream.h"

#include "vfs.h"

static int output_stream_write(output_stream_t *stream);

int output_stream_init(output_stream_t *stream,
					   bool should_create,
					   char *file_name,
					   int size,
					   int threshold,
					   int64_t offset)
{
	int flags;

	stream->buffer = (uint8_t *) vbf_malloc(size);
	if (stream->buffer == NULL) {
		vbf_set_error("out of memroy");
		return -1;
	}

	flags =  vbf_file_get_open_flags(false);
	if (should_create) {
		flags = vbf_file_get_create_flags();
	}

	stream->file = vbf_file_open(file_name, flags);
	if (stream->file == NULL) {
		return -1;
	}

	stream->threshold = threshold;
	stream->position = 0;

	stream->file_offset = offset;
	stream->file_length = offset;
	return 0;
}

int output_stream_flush(output_stream_t *stream)
{
	int result = 0;

	if (stream->position > 0) {
		if (output_stream_write(stream) == -1) {
			result = -1;
		}
	}

	return result;
}

void output_stream_fini(output_stream_t *stream)
{
	if (stream->buffer != NULL) {
		vbf_free(stream->buffer);
	}

	if (stream->file != NULL) {
		vbf_file_close(stream->file);
	}
}

int64_t output_stream_current_position(output_stream_t *stream)
{
	return stream->file_offset;
}

int64_t output_stream_next_position(output_stream_t *stream)
{
	return stream->file_offset + stream->position;
}

uint8_t *output_stream_get_buffer(output_stream_t *stream)
{
	return &stream->buffer[stream->position];
}

static int output_stream_write(output_stream_t *stream)
{

	int bytes_left;
	int bytes_written;
	int bytes_total = 0;
	uint8_t *buffer;

	buffer = stream->buffer;
	bytes_left = stream->position;
	while (bytes_left > 0) {
		bytes_written = vbf_file_write(stream->file,
									   (char *) buffer + bytes_total,
									   bytes_left,
									   stream->file_offset + bytes_total);

		if (bytes_written < 0) {
			return -1;
		}

		bytes_left -= bytes_written;
		bytes_total += bytes_written;
	}

	stream->file_offset += stream->position;
	stream->position = 0;
	return 0;
}

int output_stream_append(output_stream_t *stream, int used_length)
{
	int new_length = stream->position + used_length;

	if (new_length < stream->threshold) {
		/*
		 * Normal case -- added more data to current buffer.
		 */
		stream->position = new_length;
		stream->file_length += used_length;
		return 0;
	}

	/*
	 * Current large-write memory is full.
	 */
	stream->position = stream->threshold;
	if (output_stream_write(stream) == -1) {
		return -1;
	}

	if (new_length > stream->threshold) {
		int excess_length = new_length - stream->threshold;

		/*
		 * We have carry-over in the extra buffer.  Write and then copy
		 * the extra to the front of the large write buffer.
		 */
		memcpy(stream->buffer, stream->buffer + stream->threshold, excess_length);
		stream->position = excess_length;
	}

	stream->file_length += used_length;
	return 0;
}

int64_t output_stream_file_size(output_stream_t *stream)
{
	return stream->file_length;
}
