#ifndef _SM4_H_
#define _SM4_H_

#include "postgres.h"
#include "px.h"

typedef uint8 u1byte;
typedef uint32 u4byte;
typedef uint64 u8byte;

typedef struct _sm4_ctx
{
	u4byte		k_len;
	int			decrypt;
	u8byte		e_key[32];
	u8byte		d_key[32];
} sm4_ctx;

#define MODE_ECB 0
#define MODE_CBC 1

#define INT_MAX_KEY		(512/8)
#define INT_MAX_IV		(128/8)

struct sm4_init_ctx
{
	uint8		keybuf[INT_MAX_KEY];
	uint8		iv[INT_MAX_IV];
	union {
		sm4_ctx sm4;
	}			ctx;
	unsigned	keylen;
	int			is_init;
	int			mode;
};

void sm4_setkey_enc(sm4_ctx *ctx, u1byte* key);
void sm4_setkey_dec(sm4_ctx *ctx, u1byte* key);

void sm4_cbc_encrypt(sm4_ctx *ctx, u1byte *iva, u1byte *data, long len);
void sm4_cbc_decrypt(sm4_ctx *ctx, u1byte *iva, u1byte *data, long len);
void sm4_ecb_encrypt(sm4_ctx *ctx, u1byte *data, long len);
void sm4_ecb_decrypt(sm4_ctx *ctx, u1byte *data, long len);

PX_Cipher * sm4_load(int mode);

#endif /* _SM4_H_ */