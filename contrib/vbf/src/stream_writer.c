#include <string.h>
#include <stdlib.h>

#include "vbf/allocation.h"
#include "vbf/errors.h"
#include "vbf/stream_writer.h"

#include "vbf_private.h"

int stream_writer_init(stream_writer_t *writer,
					   const char *compress_type,
					   int compress_level,
					   int block_size,
					   char *file_name,
					   bool is_create_file,
					   int64_t file_offset)
{
	int result;
	int threshold;
	int memory_length;

	writer->block_size = block_size;
	result = vbf_codec_init(&writer->codec, compress_type, compress_level);
	if (result == -1) {
		return -1;
	}

	if (writer->codec.codec_type != CodecNULL) {
		writer->current_buffer = (uint8_t *) vbf_malloc(block_size * sizeof(uint8_t));
		if (writer->current_buffer == NULL) {
			vbf_set_error("out of memroy");
			return -1;
		}
	}

	threshold = block_size * 2;
	memory_length = block_size + threshold;

	return output_stream_init(&writer->output_stream,
							  is_create_file,
							  file_name,
							  memory_length,
							  threshold,
							  file_offset);
}

int stream_writer_flush(stream_writer_t *writer)
{
	return output_stream_flush(&writer->output_stream);
}

void stream_writer_fini(stream_writer_t *writer)
{
	if (writer->codec.codec_type != CodecNULL &&
		writer->current_buffer != NULL) {
		vbf_free(writer->current_buffer);
	}

	vbf_codec_fini(&writer->codec);
}

/*
 * When writing "short" content intended to stay within the blocksize
 * some of the buffer will be used for the  block Header.
 * This function returns that overhead length.
 *
 * Isn't the length of the Storage Block constant? NO.
 *
 * Currently, there are two things that can make it longer.  When checksums
 * are configured, we add checksum data to the header. And there is optional
 * header data (e.g. firstRowNum).
 *
 * We call the header portion with the optional checksums the fixed header
 * because we need to be able to read and evaluate the checksums before we
 * can interpret flags in the fixed header that indicate there is more header
 * information.
 *
 * The complete header length is the fixed header plus optional information.
 */

/*
 * Returns the Storage Block complete header length in bytes.
 *
 * Call this routine after specifying all optional header information for the
 * current block begin written.
 */
int stream_writer_get_header_length(stream_writer_t *writer)
{
	int	header_length;

	header_length = HEADER_REGULAR_SIZE;

	if (writer->is_rownum_set) {
		header_length += sizeof(int64_t);
	}

	return header_length;
}


/*----------------------------------------------------------------
 * Writing Small Content Efficiently that is not being Bulk Compressed
 *----------------------------------------------------------------
 */

/*
 * This section describes for writing content that is less than or equal to
 * the blocksize (e.g. 32k) bytes that is not being bulk compressed by the
 * Storage Layer.
 *
 * Actually, the content is limited to blocksize minus the header size
 */

/*
 * Get a pointer to next maximum length buffer space for
 * appending small content.
 *
 * You must decide whether you are supplying the optional first row number
 * BEFORE calling this routine!
 *
 * NOTE: The maximum length buffer space =
 *				maxBufferLen +
 *				stream_writer_get_header_length(...)
 *
 * When compression is not being used, this interface provides a pointer
 * directly into the write buffer for efficient data generation.  Otherwise,
 * a pointer to a temporary buffer to collect the uncompressed contents will
 * be provided.
 *
 * Returns NULL when the current file does not have enough room for another
 * buffer.
 */
uint8_t *stream_writer_get_buffer(stream_writer_t *writer)
{
	/*
	 * Both headers (Small and NonBulkDense) have the same length. BulkDense
	 * is a long header.
	 */
	writer->header_length = stream_writer_get_header_length(writer);

	/*
	 * If compression configured, the supply the temporary buffer instead.
	 */
	if (writer->codec.codec_type != CodecNULL) {
		return writer->current_buffer;
	}

	writer->current_buffer = output_stream_get_buffer(&writer->output_stream);
	return &writer->current_buffer[writer->header_length];
}

static int stream_writer_compress_append(stream_writer_t *writer,
										 uint8_t *source_data,
										 int source_length,
										 int block_kind,
										 int item_count,
										 int *compressed_length,
										 int *buffer_length)
{
	uint8_t *header;
	uint8_t *data_buffer;
	int      result;
	int      data_length;
	int      data_buffer_length;
	int      data_roundedup_length;

	header = output_stream_get_buffer(&writer->output_stream);
	writer->header_length = stream_writer_get_header_length(writer);

	data_buffer = &header[writer->header_length];
	data_buffer_length = writer->block_size - writer->header_length;

	result = vbf_codec_encode(&writer->codec,
							  source_data,
							  source_length,
							  data_buffer,
							  data_buffer_length,
							  compressed_length);
	if (result == -1) {
		return -1;
	}

	/*
	 * We always store the data compressed if the compressed length is less
	 * than the uncompressed length.
	 */
	data_length = *compressed_length;
	if (data_length >= source_length) {
		data_length = source_length;
		memcpy(data_buffer, source_data, source_length);
		*compressed_length = 0;
	}

	data_roundedup_length = storage_round_up(data_length);
	storage_zero_pad(data_buffer, data_length, data_roundedup_length);

	block_make_small_header(header,
							writer->is_rownum_set,
							writer->rownum,
							block_kind,
							item_count,
							source_length,
							*compressed_length);

	*buffer_length = writer->header_length + data_roundedup_length;
	return 0;
}

int stream_writer_append(stream_writer_t *writer,
						 int content_length,
						 int block_kind,
						 int row_count)
{
	int buffer_length;

	if (content_length > writer->block_size - writer->header_length) {
		vbf_set_error("content too large storage block (content length = %d, maximum buffer"
					  " length %d, complete header length %d, first row number is set %s",
					  content_length, writer->block_size, writer->header_length,
					  (writer->is_rownum_set ? "true" : "false"));
		return -1;
	}

	if (writer->codec.codec_type == CodecNULL) {
		uint8_t *header;
		uint8_t *data;
		int      roundedup_length;

		header = writer->current_buffer;
		data = &header[writer->header_length];

		roundedup_length = storage_round_up(content_length);
		storage_zero_pad(data, content_length, roundedup_length);

		block_make_small_header(header,
								writer->is_rownum_set,
								writer->rownum,
								block_kind,
								row_count,
								content_length,
								0);

		buffer_length = writer->header_length + roundedup_length;
	} else {
		int compressed_length = 0;

		if (stream_writer_compress_append(writer,
										  writer->current_buffer,
										  content_length,
										  block_kind,
										  row_count,
										  &compressed_length,
										  &buffer_length) == -1) {
			return -1;
		}
	}

	if (output_stream_append(&writer->output_stream, buffer_length) == -1) {
		return -1;
	}

	writer->header_length = 0;
	writer->is_rownum_set = false;

	return 0;
}

static int stream_writer_write_fragment(stream_writer_t *writer,
										uint8_t *content,
										int content_length,
										int block_kind,
										int row_count)
{
	uint8_t *data;
	int      buffer_length;
	int      compressed_length;

	if (writer->codec.codec_type == CodecNULL) {
		data = stream_writer_get_buffer(writer);
		memcpy(data, content, content_length);
		return stream_writer_append(writer, content_length, block_kind, row_count);
	}

	if (stream_writer_compress_append(writer,
									  content,
									  content_length,
									  block_kind,
									  row_count,
									  &compressed_length,
									  &buffer_length) == -1) {
		return -1;
	}

	if (output_stream_append(&writer->output_stream, buffer_length) == -1) {
		return -1;
	}

	writer->header_length = 0;
	return 0;
}

static int stream_writer_write_fragments(stream_writer_t *writer,
										 uint8_t *content,
										 int content_length,
										 int block_kind)
{
	int header_length;
	int max_content_length;
	int	small_content_length;
	int countdown_content_length;
	uint8_t *content_next;

	/*
	 * Now write the fragments as type Block.
	 */
	writer->is_rownum_set = false;
	header_length = stream_writer_get_header_length(writer);
	max_content_length = writer->block_size - header_length;
	countdown_content_length = content_length;
	content_next = content;

	while (true)
	{
		if (countdown_content_length <= max_content_length) {
			small_content_length = countdown_content_length;
		} else {
			small_content_length = max_content_length;
		}

		if (stream_writer_write_fragment(writer,
										 content_next,
										 small_content_length,
										 block_kind,
										 0) == -1) {
			return -1;
		}

		countdown_content_length -= small_content_length;
		if (countdown_content_length == 0)
			break;

		content_next += small_content_length;
	}

	return 0;
}

int stream_writer_large_append(stream_writer_t *writer,
							   uint8_t *content,
							   int content_length,
							   int block_kind,
							   int row_count)
{
	uint8_t *header;
	int header_length;

	header_length = stream_writer_get_header_length(writer);
	if (content_length <= writer->block_size - header_length) {
		/*
		 * This is "small" content.
		 */
		if (stream_writer_write_fragment(writer,
										 content,
										 content_length,
										 block_kind,
										 row_count) == -1) {
			return -1;
		}

		writer->is_rownum_set = false;
		return 0;
	}

	/*
	 * First is a LargeContent header-only block that has the "large"
	 * content length and "large" row count.
	 */
	header = output_stream_get_buffer(&writer->output_stream);
	block_make_large_header(header,
							writer->is_rownum_set,
							writer->rownum,
							block_kind,
							row_count,
							content_length);

	if (output_stream_append(&writer->output_stream, header_length) == -1) {
		return -1;
	}
	writer->header_length = 0;

	return stream_writer_write_fragments(writer, content, content_length, block_kind);
}

void stream_writer_reset(stream_writer_t *writer)
{
	writer->header_length = 0;
	writer->is_rownum_set = false;
}

/*----------------------------------------------------------------
 * Optional: Set First Row Number
 *----------------------------------------------------------------
 */


/*
 * Normally, the first row of an Storage Block is implicit. It
 * is the last row number of the previous block + 1. However, to support
 * BTree indexes that stored TIDs in shared-memory/disk before the
 * transaction commits, we may need to not reuse row numbers of aborted
 * transactions.  So, this routine tells the Storage Layer to
 * explicitly keep the first row number. This will take up more header
 * overhead, so the get header length routine should be
 * called afterwards to get the new overhead length.
 */


/*
 * Set the first row value for the next Storage Block to be
 * written.  Only applies to the next block.
 */
void stream_writer_set_rownum(stream_writer_t *writer, int64_t rownum)
{
	writer->rownum = rownum;

	if (rownum >= 0) {
		writer->is_rownum_set = true;
		return;
	} 

	writer->is_rownum_set = false;
}

int64_t stream_writer_file_size(stream_writer_t *writer)
{
	return output_stream_file_size(&writer->output_stream);
}
