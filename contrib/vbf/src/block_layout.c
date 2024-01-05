#include <string.h>
#include <stdlib.h>
#include <stdbool.h>

#include "vbf_private.h"
#include "vbf/allocation.h"
#include "vbf/errors.h"
#include "vbf/block_layout.h"

#define MAX_2BYTE_OFFSET (63 * 1024)

static int block_reader_item_length(block_reader_t *reader, int item_index);
static int block_get_offset(block_header_t *header, int offset, int item_index);

int block_maker_init(block_maker_t *maker,
					 uint8_t *buffer,
					 int buffer_length,
					 int scratch_buffer_length,
					 uint32_t dbid,
					 crypto_vbf_callback callback)
{
	bool is_encrypt = false;
	uint16_t dek_version = 0;

	maker->header = (block_header_t *) buffer;
	maker->buffer_length = buffer_length;
	maker->scratch_buffer = vbf_malloc(scratch_buffer_length);
	if (maker->scratch_buffer == NULL) {
		vbf_set_error("out of memory");
		return -1;
	}

	maker->scratch_buffer_length = scratch_buffer_length;
	maker->last2byte_offset_ptr = &buffer[MAX_2BYTE_OFFSET];
	maker->dbid = dbid;
	maker->callback = callback;
	maker->is_encrypt = false;

	if (maker->callback)
	 	maker->callback(1 /* operation type for get dek info */,
						maker->dbid,
						&is_encrypt,
						&dek_version,
						NULL,
						0);
	if (is_encrypt)
	{
		maker->is_encrypt = true;
		maker->dek_version = dek_version;
	}

	block_maker_reset(maker, buffer);
	return 0;
}

void block_maker_reset(block_maker_t *maker, uint8_t *buffer)
{

	int		dekVersionOffset = 0;	
	if (maker->is_encrypt)
	{
		/*
		 * if encrypt is enable, store the dek version after blocker header
		 */
		dekVersionOffset = storage_round_up(sizeof(maker->dek_version));
	}

	maker->header = (block_header_t *) buffer;
	maker->item_length = dekVersionOffset;
	maker->next_item = &buffer[BLOCK_HEADER_LEN + dekVersionOffset];
	maker->item_count = 0;
	maker->max_item_count = maker->scratch_buffer_length / 2;

	memset(buffer, 0, BLOCK_HEADER_LEN + dekVersionOffset);
	block_set_version(maker->header, VbfInitialVersion);
	block_set_small_offset(maker->header, true);
	
	if (maker->is_encrypt)
	{
		block_set_is_encrypt(maker->header, true);
		memcpy(&buffer[BLOCK_HEADER_LEN], &maker->dek_version, sizeof(maker->dek_version));
	}

}

void block_maker_fini(block_maker_t *maker)
{
	if (maker->scratch_buffer != NULL) {
		vbf_free(maker->scratch_buffer);
	}
}

uint8_t *block_maker_next_item(block_maker_t *maker, int item_length)
{
	int       item_count;
	uint8_t  *buffer;
	uint8_t  *next_item;
	int       next_item_offset;
	int       new_item_length;
	int       offset_length_rounded;
	int       new_total_length;
	uint8_t  *scratch_buffer;
	uint16_t *offset_array16;
	block_byte_offset24_t offset24;

	item_count = maker->item_count;
	if (item_count >= maker->max_item_count) {
		return NULL;
	}

	buffer = (uint8_t *) maker->header;
	next_item = maker->next_item;
	next_item_offset = (int) (next_item - buffer);
	new_item_length = maker->item_length + item_length;
	scratch_buffer = maker->scratch_buffer;

	if (block_are_small_offset(maker->header)) {
		int new_max_item_count;

		if (next_item > maker->last2byte_offset_ptr) {
			int r;

			offset_length_rounded = (item_count + 1) * BLOCK_BYTE_OFFSET_24_LEN;
			offset_length_rounded = ((offset_length_rounded + 1) / 2) * 2;
			new_total_length = BLOCK_HEADER_LEN + ((new_item_length + 1) / 2) * 2 + offset_length_rounded;

			if (new_total_length > maker->buffer_length) {
				return NULL;
			}

			new_max_item_count = maker->scratch_buffer_length / BLOCK_BYTE_OFFSET_24_LEN;
			if (item_count >= new_max_item_count) {
				return NULL;
			}

			block_set_small_offset(maker->header, false);
			maker->max_item_count = new_max_item_count;

			offset_array16 = (uint16_t *) scratch_buffer;
			for (r = item_count; r >= 0; r--) {
				offset24.byte_offset24 = offset_array16[r];
				memcpy(scratch_buffer + (r * BLOCK_BYTE_OFFSET_24_LEN), &offset24, BLOCK_BYTE_OFFSET_24_LEN);
			}

			offset24.byte_offset24 = next_item_offset;
			memcpy(scratch_buffer + (item_count * BLOCK_BYTE_OFFSET_24_LEN), &offset24, BLOCK_BYTE_OFFSET_24_LEN);
		} else {
			new_total_length = BLOCK_HEADER_LEN + ((new_item_length + 1) / 2) * 2 + item_count * 2 + 2;
			if (new_total_length > maker->buffer_length) {
				return NULL;
			}

			offset_array16 = (uint16_t *) scratch_buffer;
			offset_array16[item_count] = (uint16_t) next_item_offset;
		}
	} else {
		offset_length_rounded = (item_count + 1) * BLOCK_BYTE_OFFSET_24_LEN;
		offset_length_rounded = ((offset_length_rounded + 1) / 2) * 2;

		new_total_length = BLOCK_HEADER_LEN + ((new_item_length + 1) / 2) * 2 + offset_length_rounded;

		if (new_total_length > maker->buffer_length) {
			return NULL;
		}

		offset24.byte_offset24 = next_item_offset;
		memcpy(scratch_buffer + (item_count * BLOCK_BYTE_OFFSET_24_LEN), &offset24, BLOCK_BYTE_OFFSET_24_LEN);
	}

	maker->next_item += item_length;
	maker->item_count++;
	maker->item_length = new_item_length;

	return next_item;
}

int block_maker_item_count(block_maker_t *maker)
{
	return maker->item_count;
}

int block_maker_form(block_maker_t *maker)
{
	uint8_t *buffer;
	int      item_count;
	int      item_length;
	uint8_t *scratch_buffer;
	int      array_offset;
	int      z;
	int      multiplier;
	int      data_length;
	int      offset_array_length;
	int      offset_length_rounded;

	buffer = (uint8_t *) maker->header;
	item_count = maker->item_count;
	item_length = maker->item_length;
	scratch_buffer = maker->scratch_buffer;
	array_offset = BLOCK_HEADER_LEN + ((item_length + 1) / 2) * 2;

	if (block_are_small_offset(maker->header)) {
		multiplier = 2;
		memcpy(&buffer[array_offset], scratch_buffer, item_count * 2);
	} else {
		multiplier = BLOCK_BYTE_OFFSET_24_LEN;
		memcpy(&buffer[array_offset], scratch_buffer, item_count * BLOCK_BYTE_OFFSET_24_LEN);
	}

	for (z = BLOCK_HEADER_LEN + item_length; z < array_offset; z++) {
		buffer[z] = 0;
	}

	block_set_item_length(maker->header, item_length);
	block_set_item_count(maker->header, item_count);

	offset_array_length = item_count * multiplier;
	offset_length_rounded = ((offset_array_length + 1) / 2) * 2;

	for (z = array_offset + offset_array_length; z < array_offset + offset_length_rounded; z++) {
		buffer[z] = 0;
	}

	data_length = array_offset + offset_length_rounded;

	return data_length;
}

static int block_get_offset(block_header_t *header, int offset, int item_index)
{
	block_byte_offset24_t offset24;
	uint8_t *array_offset = ((uint8_t *) header) + offset;

	if (block_are_small_offset(header)) {
		return ((uint16_t *) array_offset)[item_index];
	}

	memcpy(&offset24, array_offset + (item_index * BLOCK_BYTE_OFFSET_24_LEN), BLOCK_BYTE_OFFSET_24_LEN);
	return offset24.byte_offset24;
}

block_check_error_t block_header_is_valid(uint8_t *buffer, int peek_length)
{
	block_header_t *header = (block_header_t *) buffer;

	VBF_UNUSED(peek_length);

	if (block_get_version(header) != VbfInitialVersion) {
		vbf_set_error("Invalid initial version %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   block_get_version(header), header->bytes_0_3, header->bytes_4_7);
		return BlockCheckBadVersion;
	}

	if (block_get_reserved(header) != 0) {
		vbf_set_error("Reserved not 0 (bytes_0_3 0x%08x, bytes_4_7 0x%08x)", header->bytes_0_3, header->bytes_4_7);
		return BlockCheckReservedNot0;
	}

	if (block_get_more_reserved(header) != 0) {
		vbf_set_error("More reserved not 0 (bytes_0_3 0x%08x, bytes_4_7 0x%08x)", header->bytes_0_3, header->bytes_4_7);
		return BlockCheckMoreReservedNot0;
	}

	return BlockCheckOk;
}

block_check_error_t block_is_valid(uint8_t *buffer, int buffer_length, bool is_encrypt)
{
	int i;
	int z;
	int offset;
	int multiplier;
	int calc_array_length;
	int calc_array_length_rounded;
	int length_rounded;
	int item_length;
	int item_count;
	int array_offset;
	int header_length = BLOCK_HEADER_LEN;
	block_check_error_t error_code;
	block_header_t *header = (block_header_t *) buffer;
	int OffsetStart;

	error_code = block_header_is_valid(buffer, buffer_length);
	if (error_code != BlockCheckOk) {
		return error_code;
	}

	item_length = block_get_item_length(header);
	if (item_length >= buffer_length) {
		vbf_set_error("item_length %d greater than or equal to buffer_length %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   item_length, buffer_length, header->bytes_0_3, header->bytes_4_7);
		return BlockCheckItemSumLenBad1;
	}

	array_offset = BLOCK_HEADER_LEN + ((item_length + 1) / 2) * 2;
	if (array_offset > buffer_length) {
		vbf_set_error("array_offset %d greater than buffer_length %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   array_offset, buffer_length, header->bytes_0_3, header->bytes_4_7);
		return BlockCheckItemSumLenBad2;
	}

	for (z = BLOCK_HEADER_LEN + item_length; z < array_offset; z++) {
		if (buffer[z] != 0) {
			vbf_set_error("bad zero pad at offset %d between items and offset array (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
						   z, header->bytes_0_3, header->bytes_4_7);
			return BlockCheckZeroPadBad1;
		}
	}

	item_count = block_get_item_count(header);
	if (item_count == 0) {
		if (array_offset != buffer_length) {
			vbf_set_error("array_offset %d should equal buffer_length %d for item_count 0 (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
						   array_offset, buffer_length, header->bytes_0_3, header->bytes_4_7);
			return BlockCheckItemCountBad1;
		}

		return BlockCheckOk;
	}

	length_rounded = buffer_length - array_offset;

	if (block_are_small_offset(header)) {
		multiplier = 2;
	} else {
		multiplier = BLOCK_BYTE_OFFSET_24_LEN;
	}

	calc_array_length = item_count * multiplier;
	calc_array_length_rounded = ((calc_array_length + 1) / 2) * 2;
	if (length_rounded != calc_array_length_rounded) {
		vbf_set_error("actual array_offset length %d should equal calculated array_offset length %d for item_count %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   length_rounded, calc_array_length_rounded, item_count, header->bytes_0_3, header->bytes_4_7);
		return BlockCheckItemCountBad2;
	}

	/*
	 * If tde enable, dek store right after the header.
	 * So we should calculate the really offset.
	 */
	OffsetStart = BLOCK_HEADER_LEN;
	if (is_encrypt)
		OffsetStart += storage_round_up(sizeof(uint16_t));

	offset = block_get_offset(header, array_offset, 0);
	if (offset != OffsetStart) {
		vbf_set_error("offset %d at index 0 is bad -- must equal %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
					   offset, header_length, header->bytes_0_3, header->bytes_4_7);
		return BlockCheckOffsetBad1;
	}

	for (i = 1; i < item_count; i++) {
		int prev_offset = offset;
		offset = block_get_offset(header, array_offset, i);
		if (offset < prev_offset) {
			vbf_set_error("offset %d at index %d is bad -- less than previous offset %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
						   offset, i, prev_offset, header->bytes_0_3, header->bytes_4_7);
			return BlockCheckOffsetBad2;
		}

		if (offset > (int) (item_length + BLOCK_HEADER_LEN)) {
			vbf_set_error("offset %d at index %d is bad -- greater than item_length and header %d (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
						   offset, i, item_length + header_length, header->bytes_0_3, header->bytes_4_7);
			return BlockCheckOffsetBad3;
		}
	}

	for (z = array_offset + calc_array_length; z < array_offset + calc_array_length_rounded; z++) {
		if (buffer[z] != 0) {
			vbf_set_error("bad zero pad at offset %d between last offset and the rounded-up end of buffer (bytes_0_3 0x%08x, bytes_4_7 0x%08x)",
						   z, header->bytes_0_3, header->bytes_4_7);
			return BlockCheckZeroPadBad2;
		}
	}

	return BlockCheckOk;
}

int block_reader_init(block_reader_t *reader, uint8_t *buffer, int buffer_length)
{
	int offset;
	int	divisor;
	int item_length;
	int calculated_count;
	block_header_t *header = (block_header_t *) buffer;

	reader->header = header;
	reader->buffer_length = buffer_length;

	item_length = block_get_item_length(header);

	offset = BLOCK_HEADER_LEN + ((item_length + 1) / 2) * 2;
	if (block_are_small_offset(header)) {
		divisor = 2;
	} else {
		divisor = BLOCK_BYTE_OFFSET_24_LEN;
	}

	calculated_count = (buffer_length - offset) / divisor;
	if (calculated_count != (int) block_get_item_count(header)) {
		vbf_set_error("item count in header %d should equal calculated item count %d",
					  calculated_count, (int) block_get_item_count(header));
		return -1;
	}

	reader->array_offset = offset;
	reader->next_index = 0;
	reader->next_item = &buffer[BLOCK_HEADER_LEN];

	return 0;
}

static int block_reader_item_length(block_reader_t *reader, int item_index)
{
	int item_length;

	block_reader_get_item(reader, item_index, &item_length);
	return item_length;
}

uint8_t *block_reader_next_item(block_reader_t *reader, int *item_length)
{
	uint8_t *next_item;

	if (reader->next_index >= (int) block_get_item_count(reader->header)) {
		return NULL;
	}

	*item_length = block_reader_item_length(reader, reader->next_index);
	next_item = reader->next_item;

	reader->next_item += *item_length;
	reader->next_index++;

	return next_item;
}

int block_reader_item_count(block_reader_t *reader)
{
	return block_get_item_count(reader->header);
}

/*
 * Get a pointer to a variable-length item.
 */
uint8_t *
block_reader_get_item(block_reader_t *reader, int item_index, int *item_length)
{
	int offset;
	int next_offset;
	block_header_t *header = reader->header;
	uint8_t *buffer = (uint8_t *) header;

	offset = block_get_offset(header, reader->array_offset, item_index);
	if (item_index < (int) (block_get_item_count(header) - 1)) {
		next_offset = block_get_offset(header, reader->array_offset, item_index + 1);
		*item_length = next_offset - offset;
	} else {
		*item_length = BLOCK_HEADER_LEN + block_get_item_length(header) - offset;
	}

	return &buffer[offset];
}

int block_make_single_item(uint8_t *target, uint8_t *source, int source_length, block_maker_t *maker)
{
	block_reader_t reader;
	uint8_t *item;
	int item_length;
	int EncryptDataLen = 0;

	if (block_reader_init(&reader, source, source_length) == -1) {
		return -1;
	}

	item = block_reader_get_item(&reader, 0, &item_length);

	/* store the dek version before the item */
	if (maker->is_encrypt)
	{
		memcpy(target, &maker->dek_version, sizeof(maker->dek_version));
		EncryptDataLen = storage_round_up(sizeof(maker->dek_version));
	}

	/*
	 * Slide data back. Since we have overlapping data, we use memmove which
	 * knows how to do that instead of memcpy which isn't guaranteed to do
	 * that right.
	 */
	memmove(target + EncryptDataLen, item, item_length);


	if (maker->is_encrypt)
	{
		maker->callback(2,
				maker->dbid,
				NULL,
				&maker->dek_version,
				target + EncryptDataLen,
				item_length);
	}

	return item_length + EncryptDataLen;
}
