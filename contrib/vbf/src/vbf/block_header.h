#ifndef VBF_BLOCK_HEADER_H
#define VBF_BLOCK_HEADER_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

#include <stdint.h>
#include <stdbool.h>

#define HEADER_REGULAR_SIZE 8
#define SMALL_HEADER_MAX_COUNT 0x3FFF   
							   // 14 bits, or 16,383 (16k-1).
							   // Maximum row count for small content.
#define storage_round_up4(l) ((((l) + 3) / 4) * 4) /* 32-bit alignment */
#define storage_round_up8(l) ((((l) + 7) / 8) * 8) /* 64-bit alignment */
#define storage_round_up(l) (storage_round_up8(l))
#define	storage_zero_pad(\
			buf,\
			len,\
			padToLen)\
{\
	int b;\
\
	for (b = len; b < padToLen; b++)\
		buf[b] = 0;\
}

typedef enum header_kind
{
	HeaderKindNone = 0,
	HeaderKindSmallContent = 1,
	HeaderKindLargeContent = 2,
	HeaderKindMax /* must always be last */
} header_kind_t;

typedef struct header
{
	uint32_t bytes_0_3;
	uint32_t bytes_4_7;
} header_t;

#define header_get_reserved0(h)    ((h)->bytes_0_3 >> 31)                 // 1 bit
#define header_get_header_kind(h)  (((h)->bytes_0_3 & 0x70000000) >> 28)    // 3 bit

typedef struct small_content_header
{
	uint32_t bytes_0_3;
	uint32_t bytes_4_7;
} small_content_header_t;

#define sch_get_reserved0(h)          ((h)->bytes_0_3 >> 31)                  // 1 bit
#define sch_get_header_kind(h)         (((h)->bytes_0_3 & 0x70000000) >> 28)    // 3 bits
#define sch_get_has_rownum(h)     (((h)->bytes_0_3 & 0x08000000) >> 27)    // 1 bits
#define sch_get_block_kind(h)  (((h)->bytes_0_3 & 0x07000000) >>24)    // 3 bits
#define sch_get_rowcount(h)           (((h)->bytes_0_3 & 0x00FFFC00) >>10)    // 14 bits
#define sch_get_data_length(h) \
          ((((h)->bytes_0_3 & 0x000003FF) << 11) | (((h)->bytes_4_7 & 0xFFE00000) >> 21))
                         // top 10 bits  lower 11 bits   21 bits
#define sch_get_compressed_length(h)   ((h)->bytes_4_7 & 0x001FFFFF)         // 21 bits

// For single bits, set or clear directly.  Otherwise, use AND to clear field then OR to set.
#define sch_set_reserved0(h, e) \
		{if(e) (h)->bytes_0_3 |= 0x80000000; else (h)->bytes_0_3 &= 0x7FFFFFFF;}
#define sch_set_header_kind(h, e) \
		{(h)->bytes_0_3 &= 0x8FFFFFFF; (h)->bytes_0_3 |= (0x70000000&((e) << 28));}
#define sch_set_has_rownum(h, e) \
		{if(e) (h)->bytes_0_3 |= 0x08000000; else(h)->bytes_0_3 &= 0xF7FFFFFF;}
#define sch_set_block_kind(h, e) \
		{(h)->bytes_0_3 &= 0xF8FFFFFF; (h)->bytes_0_3 |= (0x07000000 & ((e) << 24));}
#define sch_set_rowcount(h, e) \
		{(h)->bytes_0_3 &= 0xFF0003FF; (h)->bytes_0_3 |= (0x00FFFC00 & ((e) << 10));}
#define sch_set_data_length(h, e) \
		{(h)->bytes_0_3 &= 0xFFFFFC00; (h)->bytes_0_3 |= (0x000003FF & ((e)>>11)); \
		 (h)->bytes_4_7 &= 0x001FFFFF; (h)->bytes_4_7 |= (0xFFE00000 & ((e)<<21));}
#define sch_set_compressed_length(h, e) \
		{(h)->bytes_4_7 &= 0xFFE00000; (h)->bytes_4_7|= (0x001FFFFF & (e));}

// Assume field is initially zero.
#define sch_init_init(h)                {(h)->bytes_0_3 = 0; (h)->bytes_4_7 = 0;} 
#define sch_init_reserved0(h, e)         {if(e) (h)->bytes_0_3 |= 0x80000000;} 
#define sch_init_header_kind(h, e)        {(h)->bytes_0_3 |= (0x70000000 & ((e) << 28));}
#define sch_init_has_rownum(h, e)    {if(e) (h)->bytes_0_3 |= 0x08000000;} 
#define sch_init_block_kind(h, e) {(h)->bytes_0_3 |= (0x07000000 & ((e) << 24));}
#define sch_init_rowcount(h, e)          {(h)->bytes_0_3 |= (0x00FFFC00 & ((e) << 10));}
#define sch_init_data_length(h, e)        {(h)->bytes_0_3 |= (0x000003FF & (e >> 11)); \
                                               (h)->bytes_4_7 |= (0xFFE00000 & ((e) << 21));}
#define sch_init_compressed_length(h, e)  {(h)->bytes_4_7 |= (0x001FFFFF & (e));}


typedef struct large_content_header
{
	uint32_t bytes_0_3;
	uint32_t bytes_4_7;

} large_content_header_t;

#define lch_get_reserved0(h)          ((h)->bytes_0_3>>31)                  // 1 bit
#define lch_get_header_kind(h)         (((h)->bytes_0_3&0x70000000)>>28)    // 3 bits
#define lch_get_has_rownum(h)     (((h)->bytes_0_3&0x08000000)>>27)    // 1 bits
#define lch_get_block_kind(h)  (((h)->bytes_0_3&0x07000000)>>24)    // 3 bits
#define lch_get_reserved1(h)          (((h)->bytes_0_3&0x00800000)>>23)    // 1 bit
#define lch_get_large_rowcount(h) \
          ((((h)->bytes_0_3&0x007FFFFF)<<2)|(((h)->bytes_4_7&0xC0000000)>>30))
                         // top 23 bits  lower 2 bits   25 bits
#define lch_get_large_content_length(h)   ((h)->bytes_4_7&0x3FFFFFFF)         // 30 bits

// For single bits, set or clear directly.  Otherwise, use AND to clear field then OR to set.
#define lch_set_reserved0(h,e) \
		{if(e)(h)->bytes_0_3|=0x80000000;else(h)->bytes_0_3&=0x7FFFFFFF;} 
#define lch_set_header_kind(h,e) \
		{(h)->bytes_0_3&=0x8FFFFFFF;(h)->bytes_0_3|=(0x70000000&((e)<<28));}
#define lch_set_has_rownum(h,e) \
		{if(e)(h)->bytes_0_3|=0x08000000;else(h)->bytes_0_3&=0xF7FFFFFF;}
#define lch_set_block_kind(h,e) \
		{(h)->bytes_0_3&=0xF8FFFFFF;(h)->bytes_0_3|=(0x07000000&((e)<<24));}
#define lch_set_reserved1(h,e) \
		{if(e)(h)->bytes_0_3|=0x00800000;else(h)->bytes_0_3&=0xFF7FFFFF;} 
#define lch_set_large_rowcount(h,e) \
		{(h)->bytes_0_3&=0xFF800000;(h)->bytes_0_3|=(0x007FFFFF&(e>>2)); \
		(h)->bytes_4_7&=0x3FFFFFFF;(h)->bytes_4_7|=(0xC0000000&((e)<<30));}
#define lch_set_large_content_length(h,e) \
		{(h)->bytes_4_7&=0xC0000000;(h)->bytes_4_7|=(0x3FFFFFFF&(e));}

// Assume field is initially zero.
#define lch_init_init(h)                {(h)->bytes_0_3=0;(h)->bytes_4_7=0;} 
#define lch_init_reserved0(h,e)         {if(e)(h)->bytes_0_3|=0x80000000;} 
#define lch_init_header_kind(h,e)        {(h)->bytes_0_3|=(0x70000000&((e)<<28));}
#define lch_init_has_rownum(h,e)    {if(e)(h)->bytes_0_3|=0x08000000;} 
#define lch_init_block_kind(h,e) {(h)->bytes_0_3|=(0x07000000&((e)<<24));}
#define lch_init_reserved1(h,e)         {if(e)(h)->bytes_0_3|=0x00800000;} 
#define lch_init_large_rowcount(h,e)     {(h)->bytes_0_3|=(0x007FFFFF&(e>>2));\
                                                        (h)->bytes_4_7|=(0xC0000000&((e)<<30));}
#define lch_init_large_content_length(h,e) {(h)->bytes_4_7|=(0x3FFFFFFF&(e));}

typedef enum header_check_error
{
	HeaderCheckOk = 0,
	HeaderCheckFirst32BitsAllZeroes,
	HeaderCheckReservedBit0Not0,
	HeaderCheckInvalidHeaderKindNone,
	HeaderCheckInvalidHeaderKind,
	HeaderCheckInvalidHeaderLen,
	HeaderCheckInvalidCompressedLen,
	HeaderCheckInvalidOverallBlockLen,
	HeaderCheckLargeContentLenIsZero
} header_check_error_t;

void block_make_small_header(uint8_t *header,
							 bool has_rownum,
							 int64_t rownum,
							 int block_kind,
							 int rowcount,
							 int32_t data_length,
							 int32_t compressed_length);
void block_make_large_header(uint8_t *header,
							 bool has_rownum,
							 int64_t rownum,
							 int block_kind,
							 int rowcount,
							 int32_t content_length);
header_check_error_t block_get_header_info(uint8_t *header_ptr,
										   header_kind_t *header_kind,
										   int32_t *header_length);
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
												 int32_t *compressed_length);
int32_t block_get_compressed_length(uint8_t *header);
header_check_error_t block_get_large_header_info(uint8_t *header,
												 int header_length,
												 int32_t *content_length,
												 int *block_kind,
												 bool *has_rownum,
												 int64_t *rownum,
												 int *rowcount);

CLOSE_EXTERN
#endif
