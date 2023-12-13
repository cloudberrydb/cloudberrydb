#include <string.h>
#include <stdlib.h>

#include "vbf/allocation.h"
#include "vbf/errors.h"
#include "vbf/stream_reader.h"

#include "vfs.h"
#include "vbf_private.h"

int stream_reader_init(stream_reader_t *reader,
					   const char *compress_type,
					   int compress_level,
					   int block_size)
{
	int result;

	result = vbf_codec_init(&reader->codec, compress_type, compress_level);
	if (result == -1) {
		return -1;
	}

	return input_stream_init(&reader->input_stream, block_size,  2 * block_size);
}

void stream_reader_fini(stream_reader_t *reader)
{
	vbf_codec_fini(&reader->codec);
	input_stream_fini(&reader->input_stream);
}

int stream_reader_reset(stream_reader_t *reader,
						char *file_name,
						int64_t file_length)
{
	if (file_length == 0) {
		vbf_set_error("file '%s' EOF must be > 0", file_name, file_length);
		return -1;
	}

	return input_stream_reset(&reader->input_stream, file_name, file_length);
}

/*----------------------------------------------------------------
 * Reading Content
 *----------------------------------------------------------------
 */

/*
 * This section describes for reading potentially long content that can be
 * up to 1 Gb long and and/or content may have been be bulk-compressed.
 *
 * If the block is small and not compressed, then it may be looked at
 * directly in the read buffer.
 *
 * Otherwise, the caller must provide other buffer space to either
 * reconstruct large content and/or to decompress content into.
 */
static int stream_reader_next_buffer(stream_reader_t *reader,
									 uint8_t **header,
									 bool *has_next)
{
	int available_length;

	/*
	 * we need to honor the file-system page boundaries here since we
	 * do not let the length information cross the boundary.
	 */
	reader->header.offset = input_stream_next_buffer_position(&reader->input_stream);
	if (input_stream_get_next_buffer(&reader->input_stream, HEADER_REGULAR_SIZE,
									 &available_length, header) == -1) {
		return -1;
	}

	if (available_length == 0) {
		/* done reading the file */
		*has_next = false;
		return 0;
	}

	if (available_length != HEADER_REGULAR_SIZE) {
		vbf_set_error("expected %d bytes and got %d bytes in file '%s', header "
					  "offset in file = %ld", HEADER_REGULAR_SIZE, available_length,
					  vbf_file_name(reader->input_stream.file), reader->header.offset);
		return -1;
	}

	return available_length;
}

/*
 * Skip zero padding to next page boundary, if necessary.
 *
 * This function is called when the file system block we are scanning has
 * no more valid data but instead is padded with zero's from the position
 * we are currently in until the end of the block. The function will skip
 * to the end of block if skipLen is -1 or skip skipLen bytes otherwise.
 */
static int stream_reader_next_header(stream_reader_t *reader,
									 uint8_t **header,
									 int *limit_length,
									 bool *has_next)
{
	int     i = 0;
	int     available_length;
	int64_t remainder_length;

	/*
	 * we need to honor the file-system page boundaries here since we
	 * do not let the length information cross the boundary.
	 */
	available_length = stream_reader_next_buffer(reader, header, has_next);
	if (available_length <= 0) {
		return available_length;
	}

	while (true) {
		if ((*header)[i] != 0) {
			break;
		}

		i++;
		if (i >= HEADER_REGULAR_SIZE) {
			available_length = stream_reader_next_buffer(reader, header, has_next);
			if (available_length <= 0) {
				return available_length;
			}

			break;
		}
	}

	remainder_length = reader->input_stream.file_length - reader->header.offset;
	if (remainder_length < reader->input_stream.block_size) {
		*limit_length = remainder_length;
	} else {
		*limit_length = reader->input_stream.block_size;
	}

	*has_next = true;
	return 0;
}

/*
 * Get information on the next Storage Block.
 *
 * Return true if another block was found.  Otherwise, we have reached the
 * end of the current segment file.
 */
static int stream_reader_next_block(stream_reader_t *reader, bool *has_next)
{
	uint8_t *header;
	int      limit_length = 0;
	header_check_error_t error_code;

	memset(&reader->header, 0, sizeof(header_info_t));

	reader->header.kind = HeaderKindNone;
	reader->header.rownum = -1;

	if (stream_reader_next_header(reader, &header, &limit_length, has_next) == -1) {
		return -1;
	}

	if (!*has_next) {
		return 0;
	}

	error_code = block_get_header_info(header, &reader->header.kind, &reader->header.length);
	if (error_code != HeaderCheckOk) {
		vbf_prefix_error("bad block header('%s'), header check error %d, detail: ",
						  vbf_file_name(reader->input_stream.file), (int) error_code);
		return -1;
	}

	if (reader->header.length > HEADER_REGULAR_SIZE) {
		int available_length;
		if (input_stream_grow_buffer(&reader->input_stream,
									 reader->header.length,
									 &available_length, &header) == -1) {
			return -1;
		}

		if (available_length != reader->header.length) {
			vbf_set_error("expected %d bytes and got %d bytes in file '%s', header "
						  "offset in file = %ld", reader->header.length, available_length,
					       vbf_file_name(reader->input_stream.file), reader->header.offset);
			return -1;
		}
	}

	switch (reader->header.kind) {
		case HeaderKindSmallContent:
			error_code = block_get_small_header_info(header, reader->header.length, limit_length,
				 &reader->header.block_length, &reader->header.content_offset,
				 &reader->header.uncompressed_length, &reader->header.block_kind,
				 &reader->header.has_rownum, &reader->header.rownum, &reader->header.row_count,
				 &reader->header.is_compressed, &reader->header.compressed_length);
			if (error_code != HeaderCheckOk) {
				vbf_prefix_error("bad block header of type small content('%s'), header check error %d, detail: ",
								  vbf_file_name(reader->input_stream.file), (int) error_code);
				return -1;
			}
			break;

		case HeaderKindLargeContent:
			error_code = block_get_large_header_info(header, reader->header.length,
				 &reader->header.uncompressed_length, &reader->header.block_kind,
				 &reader->header.has_rownum, &reader->header.rownum, &reader->header.row_count);
			if (error_code != HeaderCheckOk) {
				vbf_prefix_error("bad block header of type large content('%s'), header check error %d, detail: ",
								  vbf_file_name(reader->input_stream.file), (int) error_code);
				return -1;
			}
			reader->header.is_large = true;
			break;

		default:
			vbf_set_error("unexpected header kind %d file '%s'",
						   reader->header.kind, vbf_file_name(reader->input_stream.file));
			return -1;
	}

	return 0;
}

/*
 * Internal routine to grow the input_stream buffer to be the whole current
 * block and to get header and content pointers of current block.
 *
 * Since we are growing the input_stream buffer to the whole block, old
 * pointers to the header must be abandoned.
 *
 * Header to current block was read and verified by stream_reader_next_block.
 */
static int
stream_reader_internal_get_buffer(stream_reader_t *reader,
								  uint8_t **header,
								  uint8_t **content)
{
	int available_length;

	/*
	 * Grow the buffer to the full block length to avoid any unnecessary
	 * copying by BufferedRead.
	 */
	if (input_stream_grow_buffer(&reader->input_stream,
								 reader->header.block_length,
								 &available_length, header) == -1) {
		return -1;
	}

	if (reader->header.block_length != available_length) {
		vbf_set_error("wrong buffer length, expected %d byte length buffer and got %d, file '%s'",
				       reader->header.block_length,
					   available_length,
					   vbf_file_name(reader->input_stream.file));
		return -1;
	}

	*content = &((*header)[reader->header.content_offset]);
	return 0;
}

/*
 * Get a pointer to the *small* non-compressed content.
 *
 * This interface provides a pointer directly into the read buffer for
 * efficient data use.
 */
int stream_reader_get_buffer(stream_reader_t *reader, uint8_t **content)
{
	uint8_t *header;

	/*
	 * Fetch pointers to content.
	 */
	if (stream_reader_internal_get_buffer(reader, &header, content) == -1) {
		return -1;
	}

	return 0;
}

static int stream_reader_get_small_content(stream_reader_t *reader,
										   uint8_t *content_out,
										   int content_out_length)
{
	uint8_t *content_next;

	VBF_UNUSED(content_out_length);
	if (stream_reader_get_buffer(reader, &content_next) == -1) {
		return -1;
	}

	if (!reader->header.is_compressed) {
		memcpy(content_out, content_next, reader->header.uncompressed_length);
		return 0;
	}

	if (vbf_codec_decode(&reader->codec,
						 content_next,
						 reader->header.compressed_length,
						 content_out,
						 reader->header.uncompressed_length) == -1) {
		return -1;
	}

	return 0;
}

/*
 * Copy the large and/or decompressed content out.
 *
 * The content_length parameter value must match the content_length
 * from the get block info call.
 *
 * Note this routine will work for small non-compressed content, too.
 *
 * content	- memory to receive the contiguous content.
 * content_length - byte length of the content buffer.
 */
int stream_reader_get_content(stream_reader_t *reader,
							  uint8_t *content_out,
							  int content_out_length)
{
	uint8_t *content_next;
	int64_t  large_content_position;
	int      large_content_length;
	int      remaining_content_length;	
	int      block_read_count;
	int      regular_content_length;
	bool     has_next;

	if (content_out_length != reader->header.uncompressed_length) {
		vbf_set_error("content buffer length %d which is different than the "
					  "expected length %d", content_out_length, reader->header.uncompressed_length);
		return -1;
	}

	if (!reader->header.is_large) {
		return stream_reader_get_small_content(reader, content_out, content_out_length);
	}

	/*
	 * We have the LargeContent "metadata" AO block with the total length
	 * (already read) followed by N SmallContent blocks with the fragments
	 * of the large content.
	 */

	/*
	 * Save any values needed from the current* members since they will be
	 * modifed as we read the regular blocks.
	 */
	large_content_position = reader->header.offset;
	large_content_length = reader->header.uncompressed_length;

	content_next = content_out;
	remaining_content_length = large_content_length;
	block_read_count = 0;
	while (true)
	{
		block_read_count++;

		if (stream_reader_next_block(reader, &has_next) == -1) {
			return -1;
		}

		if (!has_next) {
			/* Unexpected end of file */
			vbf_set_error("unexpected end of file trying to read block %d of large content in file '%s',"
						  "Large content metadata block is at position %ld Large content length %d",
						  block_read_count, vbf_file_name(reader->input_stream.file),
						  large_content_position, large_content_length);
			return -1;
		}

		if (reader->header.kind != HeaderKindSmallContent) {
			/* Unexpected headerKind */
			vbf_set_error("unexpected header kind 'Block' for block %d of large content in file '%s',"
						  "Large content metadata block is at position %ld Large content length %d",
						  block_read_count, vbf_file_name(reader->input_stream.file),
						  large_content_position, large_content_length);
			return -1;
		}

		regular_content_length = reader->header.uncompressed_length;
		remaining_content_length -= regular_content_length;
		if (remaining_content_length < 0) {
			/* Too much data found??? */
			vbf_set_error("oo much data found after reading %d blocks for large content in file '%s',"
						  "Large content metadata block is at position %ld Large content length %d; extra data length %d",
						  block_read_count, vbf_file_name(reader->input_stream.file),
						  large_content_position, large_content_length, -remaining_content_length);
			return -1;
		}

		/* We can safely recurse one level here. */
		if (stream_reader_get_content(reader, content_next, regular_content_length) == -1) {
			return -1;
		}

		if (remaining_content_length == 0) {
			break;
		}

		/*
		 * Advance our pointer inside the contentOut buffer to put the
		 * next bytes.
		 */
		content_next += regular_content_length;
	}

	return 0;
}

int stream_reader_get_block_info(stream_reader_t *reader,
								 bool *has_next,
								 int *content_length,
								 int *block_kind,
								 int64_t *rownum,
								 int *row_count,
								 bool *is_large,
								 bool *is_compressed)
{
	if (stream_reader_next_block(reader, has_next) == -1) {
		return -1;
	}

	*content_length = reader->header.uncompressed_length;
	*block_kind = reader->header.block_kind;
	*rownum = reader->header.rownum;
	*row_count = reader->header.row_count;
	*is_large = reader->header.is_large;
	*is_compressed = reader->header.is_compressed;

	return 0;
}

int64_t stream_reader_get_compressed_length(stream_reader_t *reader)
{
	return reader->header.compressed_length;
}
