#include <string.h>
#include <stdlib.h>

#include "vbf/errors.h"
#include "vbf/block_header.h"

static void block_header_add_rownum(uint8_t *header, int64_t rownum)
{
	int64_t *rownum_ptr;

	rownum_ptr = (int64_t *) &header[HEADER_REGULAR_SIZE];
	*rownum_ptr = rownum;
}

void block_make_small_header(uint8_t *header,
							 bool has_rownum,
							 int64_t rownum,
							 int block_kind,
							 int rowcount,
							 int32_t data_length,
							 int32_t compressed_length)
{
	small_content_header_t *block_header = (small_content_header_t *) header;

	sch_init_init(block_header);
	sch_init_header_kind(block_header, HeaderKindSmallContent);
	sch_init_block_kind(block_header, block_kind);
	sch_init_rowcount(block_header, rowcount);
	sch_init_data_length(block_header, data_length);
	sch_init_has_rownum(block_header, has_rownum);

	if (compressed_length > 0) {
		sch_init_compressed_length(block_header, compressed_length);
	}

	/*
	 * Add the optional firstRowNum.
	 *
	 * NOTE: This is not part of the 8-byte (64-bit) header because it is so
	 * big. NOTE: And, it is not covered by the header checksum because in
	 * order to NOTE: determine if we should checksum more data we would need
	 * to examine NOTE: the header data not verified by checksum yet...
	 *
	 * So, the firstRowNum is extra data between the header (and checksums)
	 * and the content.  We must add it before computing the checksum.
	 */
	if (has_rownum) {
		block_header_add_rownum(header, rownum);
	}
}

void block_make_large_header(uint8_t *header,
							 bool has_rownum,
							 int64_t rownum,
							 int block_kind,
							 int rowcount,
							 int32_t content_length)
{
	large_content_header_t *block_header = (large_content_header_t *) header;

	lch_init_init(block_header);
	lch_init_header_kind(block_header, HeaderKindLargeContent);
	lch_init_block_kind(block_header, block_kind);
	lch_init_large_rowcount(block_header, rowcount);
	lch_init_large_content_length(block_header, content_length);
	lch_init_has_rownum(block_header, has_rownum);

	/*
	 * Add the optional firstRowNum.
	 *
	 * NOTE: This is not part of the 8-byte (64-bit) header because it is so
	 * big. NOTE: And, it is not covered by the header checksum because in
	 * order to NOTE: determine if we should checksum more data we would need
	 * to examine NOTE: the header data not verified by checksum yet...
	 *
	 * So, the firstRowNum is extra data between the header (and checksums)
	 * and the content.  We must add it before computing the checksum.
	 */
	if (has_rownum) {
		block_header_add_rownum(header, rownum);
	}
}

header_check_error_t block_get_header_info(uint8_t *header_ptr,
										   header_kind_t *header_kind,
										   int32_t *header_length)
{
	header_t *header = (header_t *) header_ptr;

	if (header->bytes_0_3 == 0) {
		vbf_set_error("block header is invalid -- first 32 bits are all zeroes (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   header->bytes_0_3, header->bytes_4_7);
		return HeaderCheckFirst32BitsAllZeroes;
	}

	if (header_get_reserved0(header) != 0) {
		vbf_set_error("block header is invalid -- reserved bit 0 of the header is not zero (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   header->bytes_0_3, header->bytes_4_7);
		return HeaderCheckReservedBit0Not0;
	}

	*header_kind = header_get_header_kind(header);
	if (*header_kind == HeaderKindNone) {
		vbf_set_error("block header is invalid -- invalid value 0 (none) for header kind (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   header->bytes_0_3, header->bytes_4_7);
		return HeaderCheckInvalidHeaderKindNone;
	}

	if (*header_kind >= HeaderKindMax) {
		vbf_set_error("block header is invalid -- invalid header kind value %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   (int) *header_kind, header->bytes_0_3, header->bytes_4_7);
		return HeaderCheckInvalidHeaderKind;
	}

	*header_length = HEADER_REGULAR_SIZE;
	if (*header_kind == HeaderKindSmallContent) {
		if (sch_get_has_rownum((small_content_header_t *) header_ptr)) {
			(*header_length) += sizeof(int64_t);
		}

		return HeaderCheckOk;
	}

	if (lch_get_has_rownum((large_content_header_t *) header_ptr)) {
		(*header_length) += sizeof(int64_t);
	}

	return HeaderCheckOk;
}

header_check_error_t block_get_small_header_info(uint8_t *header,
												 int header_length,
												 int32_t block_limit_length,
												 int32_t *overall_block_length,
												 int32_t *offset,
												 int32_t *uncompressed_length,
												 int *block_kind,
												 bool *has_rownum,
												 int64_t *rownum,
												 int *rowcount,
												 bool *is_compressed,
												 int32_t *compressed_length)
{
	int32_t length;
	small_content_header_t *block_header = (small_content_header_t *) header;

	*block_kind = sch_get_block_kind(block_header);
	*has_rownum = sch_get_has_rownum(block_header);
	*rowcount = sch_get_rowcount(block_header);

	*rownum = -1;
	*offset = HEADER_REGULAR_SIZE;

	if (*has_rownum) {
		int64_t *rownum_ptr = (int64_t *) &header[*offset];

		*rownum = *rownum_ptr;
		(*offset) += sizeof(int64_t);
	}

	if (*offset != header_length) {
		vbf_set_error("content offset %d doesn't equal header length parameter %d",
					   *offset, header_length);
		return HeaderCheckInvalidHeaderLen;
	}

	*uncompressed_length = sch_get_data_length(block_header);
	*compressed_length = sch_get_compressed_length(block_header);
	if (*compressed_length == 0) {
		*is_compressed = false;
		length = *uncompressed_length;
	} else {
		*is_compressed = true;
		length = *compressed_length;

		if (*compressed_length > *uncompressed_length) {
			vbf_set_error("block header is invalid -- compressed length %d is > uncompressed length %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
						   *compressed_length, *uncompressed_length, block_header->bytes_0_3, block_header->bytes_4_7);
			return HeaderCheckInvalidCompressedLen;
		}
	}

	*overall_block_length = *offset + storage_round_up(length);
	if (*overall_block_length > block_limit_length) {
		vbf_set_error("block header is invalid -- overall block length %d is > block limit length %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   *overall_block_length, block_limit_length, block_header->bytes_0_3, block_header->bytes_4_7);
		return HeaderCheckInvalidOverallBlockLen;
	}

	return HeaderCheckOk;
}

int32_t block_get_compressed_length(uint8_t *header)
{
	small_content_header_t *block_header = (small_content_header_t *) header;
	return sch_get_compressed_length(block_header);
}

header_check_error_t block_get_large_header_info(uint8_t *header,
												 int header_length,
												 int32_t *content_length,
												 int *block_kind,
												 bool *has_rownum,
												 int64_t *rownum,
												 int *rowcount)
{
	int32_t offset;
	large_content_header_t *block_header = (large_content_header_t *) header;

	*block_kind = lch_get_block_kind(block_header);
	*has_rownum = lch_get_has_rownum(block_header);
	*rowcount = lch_get_large_rowcount(block_header);
	*content_length = lch_get_large_content_length(block_header);
	if (*content_length == 0) {
		vbf_set_error("block header is invalid -- large content length is zero (block_bytes_0_3 0x%08x, block_bytes_4_7 0x%08x)",
					   block_header->bytes_0_3, block_header->bytes_4_7);
		return HeaderCheckLargeContentLenIsZero;
	}

	*rownum = -1;
	offset = HEADER_REGULAR_SIZE;

	if (*has_rownum) {
		int64_t *rownum_ptr = (int64_t *) &header[offset];

		*rownum = *rownum_ptr;
		offset += sizeof(int64_t);
	}

	if (offset != header_length) {
		vbf_set_error("content offset %d doesn't equal header length parameter %d", offset, header_length);
		return HeaderCheckInvalidHeaderLen;
	}

	return HeaderCheckOk;
}
