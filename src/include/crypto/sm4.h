/*-------------------------------------------------------------------------
 *
 * sm4.h
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 * src/include/crypto/sm4.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SM4_H_
#define _SM4_H_
#include "c.h"

typedef signed char int8_t;
typedef unsigned char uint8_t;
typedef short int16_t;
typedef unsigned short uint16_t;
typedef int int32_t;
typedef unsigned int uint32_t;
typedef long int64_t;
typedef unsigned long int uint64_t;

# define SM4_ENCRYPT     1
# define SM4_DECRYPT     0

# define SM4_BLOCK_SIZE    16
# define SM4_KEY_SCHEDULE  32

/* ossl_inline: portable inline definition usable in public headers */
# if !defined(inline) && !defined(__cplusplus)
#  if defined(__STDC_VERSION__) && __STDC_VERSION__>=199901L
   /* just use inline */
#   define ossl_inline inline
#  elif defined(__GNUC__) && __GNUC__>=2
#   define ossl_inline __inline__
#  elif defined(_MSC_VER)
  /*
   * Visual Studio: inline is available in C++ only, however
   * __inline is available for C, see
   * http://msdn.microsoft.com/en-us/library/z8y1yy88.aspx
   */
#   define ossl_inline __inline
#  else
#   define ossl_inline
#  endif
# else
#  define ossl_inline inline
# endif

typedef struct SM4_KEY_st {
    uint32_t rk[SM4_KEY_SCHEDULE];
} SM4_KEY;

typedef struct _sm4_ctx
{
	uint32_t	k_len;
	int			encrypt;
	SM4_KEY		rkey;
} sm4_ctx;

int ossl_sm4_set_key(const uint8_t *key, SM4_KEY *ks);

void ossl_sm4_encrypt(const uint8_t *in, uint8_t *out, const SM4_KEY *ks);

void ossl_sm4_decrypt(const uint8_t *in, uint8_t *out, const SM4_KEY *ks);
void sm4_setkey_enc(sm4_ctx *ctx, uint8_t* key);
void sm4_setkey_dec(sm4_ctx *ctx, uint8_t* key);
int sm4_ofb_cipher(sm4_ctx *ctx, unsigned char *out,
                        const unsigned char *in, size_t input_len,
                        unsigned char ivec[16]);
#endif /* _SM4_H_ */
