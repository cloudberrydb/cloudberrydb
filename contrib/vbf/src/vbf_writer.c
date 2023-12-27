#include <string.h>
#include <stdlib.h>

#include "vbf/allocation.h"
#include "vbf/errors.h"
#include "vbf/vbf_writer.h"

#include "vbf_private.h"

static int end_write_block(vbf_writer_t *writer);

int vbf_writer_init(vbf_writer_t *writer,
					const char *compress_type,
					int compress_level,
					int block_size,
					char *file_name,
					bool is_create_file,
					int64_t file_offset,
					int64_t rownum)
{
	int result;
	int header_length;

	memset(writer, 0, sizeof(vbf_writer_t));

	result = stream_writer_init(&writer->stream_writer,
								compress_type,
								compress_level,
								block_size,
								file_name,
								is_create_file,
								file_offset);
	if (result == -1) {
		return -1;
	}
	writer->rownum = rownum;

	stream_writer_set_rownum(&writer->stream_writer, writer->rownum);
	writer->buffer = stream_writer_get_buffer(&writer->stream_writer);
	header_length = stream_writer_get_header_length(&writer->stream_writer);
	return block_maker_init(&writer->block_maker,
							 writer->buffer,
							 block_size - header_length,
							 block_size / 8);
}

int vbf_writer_flush(vbf_writer_t *writer)
{
	if (end_write_block(writer) == -1) {
		return -1;
	}

	return stream_writer_flush(&writer->stream_writer);
}

void vbf_writer_fini(vbf_writer_t *writer)
{
	block_maker_fini(&writer->block_maker);
	stream_writer_fini(&writer->stream_writer);
}

void begin_write_block(vbf_writer_t *writer)
{
	stream_writer_set_rownum(&writer->stream_writer, writer->rownum);
	writer->buffer = stream_writer_get_buffer(&writer->stream_writer);
	block_maker_reset(&writer->block_maker, writer->buffer);
}

static void reset_buffer(vbf_writer_t *writer)
{
	if (!writer->buffer) {
		return;
	}

	stream_writer_reset(&writer->stream_writer);
	writer->buffer = NULL;
}

static int end_write_block(vbf_writer_t *writer)
{
	int  data_length;
	int  block_kind = BlockKindVarBlock;
	int  item_count = block_maker_item_count(&writer->block_maker);

	if (item_count == 0) {
		reset_buffer(writer);
		return 0;
	}

	data_length = block_maker_form(&writer->block_maker);
	if (item_count == 1) {
		data_length = block_make_single_item(writer->buffer, writer->buffer, data_length);
		if (data_length == -1) {
			return -1;
		}

		block_kind = BlockKindSingleRow;
	}

	if (stream_writer_append(&writer->stream_writer,
							 data_length,
							 block_kind,
							 item_count) == -1) {
		return -1;
	}

	writer->buffer = NULL;
	return 0;
}

int vbf_writer_write(vbf_writer_t *writer, uint8_t *buffer, int buffer_length)
{
	uint8_t *item;
	bool     is_large = false;

	/*
	 * If we are at the limit for storage header's row count, force this
	 * block to finish.
	 */
	if (block_maker_item_count(&writer->block_maker) >= SMALL_HEADER_MAX_COUNT) {
		item = NULL;
	} else {
		item = block_maker_next_item(&writer->block_maker, buffer_length);
	}

	/*
	 * If no more room to place items in the current block finish it and
	 * start inserting into the next one.
	 */
	if (item == NULL) {
		if (block_maker_item_count(&writer->block_maker) == 0) {
			/*
			 * Case #1.  The entire tuple cannot fit within a block.  It is
			 * too large.
			 */
			is_large = true;
		} else {
			/*
			 * Write out the current block to make room.
			 */
			if (end_write_block(writer) == -1) {
				return -1;
			}

			/*
			 * Setup a new block.
			 */
			begin_write_block(writer);
			item = block_maker_next_item(&writer->block_maker, buffer_length);
			if (item == NULL) {
				/*
				 * Case #2.  The entire tuple cannot fit within a block. It
				 * is too large.
				 */
				is_large = true;
			}
		}
	}

	if (!is_large) {
		/*
		 * We have room in the current block for the new tuple.
		 */
		if (buffer_length > 0) {
			memcpy(item, buffer, buffer_length);
		}

		writer->rownum++;
		return 0;
	}

	/*
	 * Write the large tuple as a large content multiple-block set.
	 */
	reset_buffer(writer);
	if (stream_writer_large_append(&writer->stream_writer,
								   buffer,
								   buffer_length,
								   BlockKindSingleRow, 1) == -1) {
		return -1;
	}
	begin_write_block(writer);

	writer->rownum++;
	return 0;
}

int64_t vbf_writer_file_size(vbf_writer_t *writer)
{
	return stream_writer_file_size(&writer->stream_writer);
}
