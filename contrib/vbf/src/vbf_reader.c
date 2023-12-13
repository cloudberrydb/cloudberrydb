#include <string.h>
#include <stdlib.h>

#include "vbf/allocation.h"
#include "vbf/errors.h"
#include "vbf/vbf_reader.h"

#include "vbf_private.h"

int vbf_reader_init(vbf_reader_t *reader,
					const char *compress_type,
					int compress_level,
					int block_size)
{
	memset(reader, 0, sizeof(vbf_reader_t));

	reader->uncompressed_buffer = vbf_malloc(block_size);
	if (reader->uncompressed_buffer == NULL) {
		vbf_set_error("out of memory");
		return -1;
	}

	return stream_reader_init(&reader->stream_reader,
							  compress_type,
							  compress_level,
							  block_size);
}

void vbf_reader_fini(vbf_reader_t *reader)
{
	if (reader->uncompressed_buffer) {
		vbf_free(reader->uncompressed_buffer);
	}

	if (reader->large_buffer) {
		vbf_free(reader->large_buffer);
	}

	stream_reader_fini(&reader->stream_reader);
}

int vbf_reader_reset(vbf_reader_t *reader,
					 char *file_name,
					 int64_t file_length)
{
	int result = stream_reader_reset(&reader->stream_reader, file_name, file_length);

	if (result == -1) {
		return -1;
	}

	reader->rownum = 1;
	reader->need_next_block = true;
	return 0;
}

static int vbf_reader_get_block_info(vbf_reader_t *reader, bool *has_next)
{
	int64_t rownum = reader->rownum;

	if (stream_reader_get_block_info(&reader->stream_reader,
									 has_next,
									 &reader->data_length,
									 &reader->block_kind,
									 &reader->rownum,
									 &reader->row_count,
									 &reader->is_large,
									 &reader->is_compressed) == -1) {
		return -1;
	}

	if (*has_next == false) {
		return 0;
	}

	if (reader->rownum < 0) {
		reader->rownum = rownum;
	}

	return 0;
}

static int vbf_reader_get_buffer(vbf_reader_t *reader, uint8_t **data_buffer)
{
	if (reader->is_compressed) {
		if (reader->is_large) {
			vbf_set_error("expected 'small' block and got 'large' block");
			return -1;
		}

		*data_buffer = reader->uncompressed_buffer;
		if (stream_reader_get_content(&reader->stream_reader,
									  *data_buffer,
									  reader->data_length) == -1) {
			return -1;
		}

		return 0;
	}

	if (!reader->is_large) {
		if (stream_reader_get_buffer(&reader->stream_reader, data_buffer) == -1) {
			return -1;
		}
		return 0;
	}

	if (reader->block_kind != BlockKindSingleRow) {
		vbf_set_error("expected 'BlockKindSingleRow' block and got %d block", reader->block_kind);
		return -1;
	}

	if (reader->large_buffer_length < reader->data_length) {
		if (reader->large_buffer != NULL) {
			vbf_free(reader->large_buffer);
		}

		reader->large_buffer = (uint8_t *) vbf_malloc(reader->data_length);
		if (reader->large_buffer == NULL) {
			vbf_set_error("out of memory");
			return -1;
		}
		reader->large_buffer_length = reader->data_length;
	}

	*data_buffer = reader->large_buffer;
	if (stream_reader_get_content(&reader->stream_reader,
								  *data_buffer,
								  reader->data_length) == -1) {
		return -1;
	}

	return 0;
}

static int vbf_reader_get_contents(vbf_reader_t *reader)
{
	uint8_t *data_buffer;
	block_check_error_t error_code;

	if (vbf_reader_get_buffer(reader, &data_buffer) == -1) {
		return -1;
	}

	switch (reader->block_kind) {
		case BlockKindVarBlock:
			error_code = block_is_valid(data_buffer, reader->data_length);
			if (error_code != BlockCheckOk) {
				vbf_prefix_error("block is not valid, valid block check error %d, detail:", error_code);
				return -1;
			}

			if (block_reader_init(&reader->block_reader, data_buffer, reader->data_length) == -1) {
				return -1;
			}

			reader->current_item_count = 0;
			reader->item_count = block_reader_item_count(&reader->block_reader);
			if (reader->row_count != reader->item_count) {
				vbf_set_error("row count %d in storage header does not match block item count %d",
							   reader->row_count,
							   reader->item_count);
				return -1;
			}
			break;
		case BlockKindSingleRow:
			if (reader->row_count != 1) {
				vbf_set_error("row count %d in storage header is not 1 for single row", reader->row_count);
				return -1;
			}
			reader->single_buffer = data_buffer;
			break;
		default:
			vbf_set_error("unrecognized block kind: %d", reader->block_kind);
			return -1;
	}

	return 0;
}

static bool vbf_reader_scan_next(vbf_reader_t *reader,
								 uint8_t **buffer,
								 int *buffer_length,
								 int64_t *rownum)
{
	uint8_t *item;
	int      item_length = 0;

	if (reader->block_kind == BlockKindSingleRow) {
		if (reader->single_buffer == NULL) {
			reader->rownum += reader->row_count;
			return false;
		}

		*buffer = reader->single_buffer;
		*buffer_length = reader->data_length;
		*rownum = reader->rownum;

		reader->single_buffer = NULL;
		return true;
	}

	/*
	 * get the next item (tuple) from the block
	 */
	while (true) {
		item = block_reader_next_item(&reader->block_reader, &item_length);
		if (item == NULL) {
			/* no more items in the block, get new buffer */
			reader->rownum += reader->row_count;
			return false;
		}

		reader->current_item_count++;

		*buffer = item;
		*buffer_length = item_length;
		*rownum = reader->rownum + reader->current_item_count - 1;

		return true;
	}

	return false;
}

int vbf_reader_next(vbf_reader_t *reader,
					uint8_t **buffer,
					int *buffer_length,
					bool *has_next)
{
	int64_t rownum;

	return vbf_reader_next_with_rownum(reader, buffer, buffer_length, &rownum, has_next);
}

int vbf_reader_next_with_rownum(vbf_reader_t *reader,
								uint8_t **buffer,
								int *buffer_length,
								int64_t *rownum,
								bool *has_next)
{
	bool found;
	bool has_block;

	while(true) {
		if (reader->need_next_block) {
			if (vbf_reader_get_block_info(reader, &has_block) == -1) {
				return -1;
			}

			if (!has_block) {
				*has_next = false;
				return 0;
			}

			if (vbf_reader_get_contents(reader) == -1) {
				return -1;
			}

			reader->need_next_block = false;
		}

		found = vbf_reader_scan_next(reader, buffer, buffer_length, rownum);
		if (found) {
			*has_next = true;
			return 0;
		}

		reader->need_next_block = true;
	}
	
	*has_next = false;
	return 0;
}
