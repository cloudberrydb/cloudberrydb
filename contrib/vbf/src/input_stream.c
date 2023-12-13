#include <string.h>
#include <stdlib.h>

#include "vbf/allocation.h"
#include "vbf/errors.h"
#include "vbf/input_stream.h"

#include "vfs.h"

static int input_stream_read(input_stream_t *stream);

int input_stream_init(input_stream_t *stream,
					  int block_size,
					  int threshold)
{

	stream->memory = (uint8_t *) vbf_malloc(block_size + threshold);
	if (stream->memory == NULL) {
		vbf_set_error("out of memroy");
		return -1;
	}

	stream->block_size = block_size;
	stream->threshold = threshold;

	stream->buffer = &stream->memory[block_size];
	return 0;
}

void input_stream_fini(input_stream_t *stream)
{
	if (stream->memory) {
		vbf_free(stream->memory);
		stream->memory = NULL;
	}

	if (stream->file) {
		vbf_file_close(stream->file);
		stream->file = NULL;
	}
}

int input_stream_reset(input_stream_t *stream,
					   char *file_name,
					   int64_t file_length)
{
	int flags = vbf_file_get_open_flags(true);

	if (stream->file) {
		vbf_file_close(stream->file);
	}

	stream->buffer_offset = 0;
	stream->buffer_length = 0;
	stream->read_position = 0;
	stream->file_offset = 0;

	stream->file = vbf_file_open(file_name, flags);
	if (stream->file == NULL) {
		return -1;
	}

	stream->file_length = file_length;
	if (file_length == 0) {
		stream->read_length = 0;
		return 0;
	}

	if (file_length > stream->threshold) {
		stream->read_length = stream->threshold;
	} else {
		stream->read_length = (int) file_length;
	}

	return input_stream_read(stream);
}

static int input_stream_read(input_stream_t *stream)
{
	int      offset = 0;
	uint8_t *read_memory = stream->buffer;
	int      read_length = stream->read_length;

	while (read_length > 0) {
		int actual_length;

		actual_length = vbf_file_read(stream->file,
									  (char *) read_memory,
									  read_length,
									  stream->file_offset);

		if (actual_length == 0) {
			vbf_set_error("read beyond eof in file \"%s\", read position %ld (small offset %d"
						  "), actual read length %d (large read length %d)",
						  vbf_file_name(stream->file), stream->read_position,
						  offset, actual_length, stream->read_length);
			return -1;
		} else if (actual_length < 0) {
			return -1;
		}

		stream->file_offset += actual_length;

		read_length -= actual_length;
		read_memory += actual_length;
		offset += actual_length;
	}

	return 0;
}

static uint8_t *input_stream_get_internal_buffer(input_stream_t *stream,
												 int request_length,
												 int *available_length)
{
	int64_t next_position;
	int64_t remaining_length;
	int		next_read_length;
	int		before_length;
	int		before_offset;
	int		extra_length;

	next_position = stream->read_position + stream->read_length;
	if (next_position == stream->file_length) {
		stream->buffer_length = stream->read_length - stream->buffer_offset;

		*available_length = stream->buffer_length;
		return &stream->buffer[stream->buffer_offset];
	}

	remaining_length = stream->file_length - next_position;
	if (remaining_length > stream->threshold) {
		next_read_length = stream->threshold;
	} else {
		next_read_length = (int) remaining_length;
	}

	before_length = stream->threshold - stream->buffer_offset;
	before_offset = stream->block_size - before_length;

	memcpy(&stream->memory[before_offset],
		   &stream->buffer[stream->buffer_offset], before_length);

	stream->read_position = next_position;
	stream->read_length = next_read_length;
	if (next_read_length == 0) {
		vbf_set_error("read length is set to 0 before calling input_stream_read"
					  "remaining_length is %ld file_length is %ld next_position"
					  "is %ld", remaining_length, stream->file_length, next_position);
		return NULL;
	}

	if (input_stream_read(stream) == -1) {
		return NULL;
	}

	extra_length = request_length - before_length;
	if (extra_length > next_read_length) {
		extra_length = (int) next_read_length;
	}

	/*
	 * Return a buffer using a negative offset that starts in the before
	 * memory and goes into the large-read memory.
	 */
	stream->buffer_offset = -before_length;
	stream->buffer_length = before_length + extra_length;

	*available_length = stream->buffer_length;
	return &stream->memory[before_offset];
}

/*
 * Return the position of the next read in bytes.
 */
int64_t input_stream_next_buffer_position(input_stream_t *stream)
{
	/*
	 * Note that the buffer_offset can be negative when we are using the before
	 * buffer, but that is ok.  We should get an accurate next position.
	 */
	return stream->read_position + stream->buffer_offset + stream->buffer_length;
}

/*
 * Get the next buffer space for reading with a specified max read-ahead
 * amount.
 */
int input_stream_get_next_buffer(input_stream_t *stream,
								 int request_length,
								 int *available_length,
								 uint8_t **buffer)
{
	uint8_t *result;
	int64_t remaining_length;

	if (request_length > stream->block_size) {
		vbf_set_error("read ahead length %d is greater than maximum buffer length %d",
					  request_length, stream->block_size);
		return -1;
	}

	stream->buffer_offset += stream->buffer_length;
	stream->buffer_length = 0;

	if (stream->read_position == stream->file_length) {
		*available_length = 0;
		return 0;
	}

	if (stream->buffer_offset == stream->read_length) {
		stream->buffer_offset = 0;
		stream->read_position += stream->read_length;

		remaining_length = stream->file_length - stream->read_position;
		if (remaining_length > stream->threshold) {
			stream->read_length = stream->threshold;
		} else {
			stream->read_length = (int) remaining_length;
		}

		if (input_stream_read(stream) == -1) {
			return -1;
		}

		if (request_length > stream->read_length)
			stream->buffer_length = stream->read_length;
		else
			stream->buffer_length = request_length;

		*available_length = stream->buffer_length;
		*buffer = stream->buffer;
		return 0;
	}

	if (stream->buffer_offset + request_length > stream->read_length) {
		result = input_stream_get_internal_buffer(stream,
												  request_length,
												  available_length);
		if (result == NULL) {
			return -1;
		}

		*buffer = result;
		return 0;
	}

	stream->buffer_length = request_length;

	*available_length = stream->buffer_length;
	*buffer = &stream->buffer[stream->buffer_offset];
	return 0;
}

/*
 * Grow the available length of the current buffer.
 *
 * NOTE: The buffer address returned can be different, even for previously
 * examined buffer data.  In other words, don't keep buffer pointers in
 * the buffer region.  Use offsets to re-establish pointers after this call.
 *
 * If the current file has been completely read, bufferLen will remain
 * the current value.
 */
int input_stream_grow_buffer(input_stream_t *stream,
							 int request_length,
							 int *available_length,
							 uint8_t **buffer)
{
	uint8_t *result;
	int new_next_offset;

	new_next_offset = stream->buffer_offset + request_length;
	if (new_next_offset > stream->read_length) {
		result = input_stream_get_internal_buffer(stream,
												  request_length,
												  available_length);
		if (result == NULL) {
			return -1;
		}

		*buffer = result;
		return 0;
	}

	/*
	 * There is request_length more left in current large-read memory.
	 */
	stream->buffer_length = request_length;

	*available_length = stream->buffer_length;
	*buffer = &stream->buffer[stream->buffer_offset];
	return 0;
}

/*
 * Return the address of the current read buffer.
 */
uint8_t *input_stream_get_current_buffer(input_stream_t *stream)
{
	return &stream->buffer[stream->buffer_offset];
}

/*
 * Return the current buffer's start position.
 */
int64_t input_stream_current_position(input_stream_t *stream)
{
	return stream->read_position + stream->buffer_offset;
}
