#ifndef VBF_BLOCK_LAYOUT_H
#define VBF_BLOCK_LAYOUT_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdint.h>

#define BLOCK_BYTE_OFFSET_24_LEN 3

typedef enum block_kind
{
	BlockKindNone = 0,
	BlockKindVarBlock,
	BlockKindSingleRow,
	MaxBlockKind
} block_kind_t;

typedef enum block_version
{
	VbfInitialVersion = 1
} block_version_t;

/*
 * The block_byte_offset24 is designed to be 3 bytes long
 * (it contains a 24-bit field) but a few compilers will pad it to
 * four bytes.  Rather than deal with the ambiguity of unpersuadable compilers,
 * we try to use "BLOCK_BYTE_OFFSET_24_LEN" rather than 
 * "sizeof(block_byte_offset24)" when computing on-disk sizes AND 
 * we DO NOT index arrays as the struct but calculate addesses and do byte
 * moves.
 */
typedef struct block_byte_offset24
{
	unsigned byte_offset24:24;
} block_byte_offset24_t;

typedef struct block_byte_length24
{
	unsigned byte_length24:24;
} block_byte_length24_t;

typedef struct block_header
{
/*
 * Can't seem to get this to pack nicely to 64 bits so the header is aligned
 * on an 8 byte boundary, so go to bit shifting...
 */
 
//  unsigned  		offsets_are_small:1;  
//                        /* True if the offsets are small and occupy 2 bytes.
//                         * Otherwise, the block is larger 3-byte offsets are
//                         * used.
//                         */
//  unsigned  		reserved:5;
//						   /* Reserved for future. */
//  block_version_t version:2;  
//						   /* Version number */
//
//  block_byte_length24_t item_length_sum;
//                         /* Offset to the item offset array. */
//
// -----------------------------------------------------------------------------
//  unsigned  		more_reserved:8;
//						   /* Reserved for future. */
//  unsigned  		item_count:24;
//                         /* Total number of items. 24-bits to we bit-pad 
//						    * to a total of 64-bits. 
//						    */
	uint32_t bytes_0_3;
	uint32_t bytes_4_7;

} block_header_t;

#define BLOCK_HEADER_LEN sizeof(block_header_t)

#define block_are_small_offset(h)   ((h)->bytes_0_3 >> 31)
#define block_get_reserved(h)       (((h)->bytes_0_3 & 0x7C000000) >>26)
#define block_get_version(h)        (((h)->bytes_0_3 & 0x03000000) >>24)
#define block_get_item_length(h)    ((h)->bytes_0_3 & 0x00FFFFFF)
#define block_get_more_reserved(h)  (((h)->bytes_4_7 & 0xFF000000) >>24)
#define block_get_item_count(h)     ((h)->bytes_4_7 & 0x00FFFFFF)

// For single bits, set or clear directly.  Otherwise, use AND to clear field then OR to set.
#define block_set_small_offset(h, e)  {if(e)(h)->bytes_0_3 |= 0x80000000; else (h)->bytes_0_3 &= 0x7FFFFFFF;} 
#define block_set_version(h, e)       {(h)->bytes_0_3 &= 0xFCFFFFFF; (h)->bytes_0_3 |= (0x03000000 & ((e) << 24));} 
#define block_set_item_length(h, e)   {(h)->bytes_0_3 &= 0xFF000000; (h)->bytes_0_3 |= (0x00FFFFFF & (e));}
#define block_set_item_count(h, e)    {(h)->bytes_4_7 &= 0xFF000000; (h)->bytes_4_7 |= (0x00FFFFFF & (e));}

// Assume field is initially zero.
#define block_init(h)                   {(h)->bytes_0_3 = 0; (h)->bytes_4_7 = 0;} 
#define block_init_small_offset(h, e)   {if(e) (h)->bytes_0_3 |= 0x80000000;} 
#define block_init_version(h, e)        {(h)->bytes_0_3 |= (0x03000000 & ((e) << 24));} 
#define block_init_item_length(h, e)    {(h)->bytes_0_3 |= (0x00FFFFFF & (e));}
#define block_init_item_count(h, e)     {(h)->bytes_4_7 |= (0x00FFFFFF & (e));}

// ----------------------------------------------------------
typedef struct block_maker
{
	block_header_t *header;
	int             buffer_length;
    int             item_length;
    uint8_t        *scratch_buffer;
    int             scratch_buffer_length;
    uint8_t        *next_item;
	uint8_t        *last2byte_offset_ptr;
    int             item_count;
	int             max_item_count;
} block_maker_t;

// ----------------------------------------------------------
typedef struct block_reader
{
	block_header_t *header;
	int             buffer_length;
	int             next_index;
	uint8_t        *next_item;
	int             array_offset;
} block_reader_t;

typedef enum block_check_error
{
	BlockCheckOk = 0,
	BlockCheckBadVersion,
	BlockCheckReservedNot0,
	BlockCheckMoreReservedNot0,
	BlockCheckItemSumLenBad1,
	BlockCheckItemSumLenBad2,
	BlockCheckZeroPadBad1,
	BlockCheckZeroPadBad2,
	BlockCheckItemCountBad1,
	BlockCheckItemCountBad2,
	BlockCheckOffsetBad1,
	BlockCheckOffsetBad2,
	BlockCheckOffsetBad3,
} block_check_error_t;

int block_maker_init(block_maker_t *maker,
					 uint8_t *buffer,
					 int buffer_length,
					 int scratch_buffer_length);
void block_maker_reset(block_maker_t *maker, uint8_t *buffer);
uint8_t *block_maker_next_item(block_maker_t *maker, int item_length);
int block_maker_item_count(block_maker_t *maker);
int block_maker_form(block_maker_t *maker);
void block_maker_fini(block_maker_t *maker);

block_check_error_t block_header_is_valid(uint8_t *buffer, int peek_length);
block_check_error_t block_is_valid(uint8_t *buffer, int buffer_length);
int block_make_single_item(uint8_t *target, uint8_t *source, int source_length);
int block_reader_init(block_reader_t *reader, uint8_t *buffer, int buffer_length);
uint8_t *block_reader_next_item(block_reader_t *reader, int *item_length);
int block_reader_item_count(block_reader_t *reader);
uint8_t *block_reader_get_item(block_reader_t *reader, int item_index, int *item_length);

CLOSE_EXTERN
#endif
