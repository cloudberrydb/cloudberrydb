/*-------------------------------------------------------------------------
 * cipher_openssl.c
 *		Cryptographic function using OpenSSL
 *
 * This contains the common low-level functions needed in both frontend and
 * backend, for implement the database encryption.
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/cipher_openssl.c
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/cipher.h"
#include <openssl/conf.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ssl.h>

/*
 * prototype for the EVP functions that return an algorithm, e.g.
 * EVP_aes_128_gcm().
 */
typedef const EVP_CIPHER *(*ossl_EVP_cipher_func) (void);

static ossl_EVP_cipher_func get_evp_aes_gcm(int klen);
static EVP_CIPHER_CTX *ossl_cipher_ctx_create(int cipher, unsigned char *key,
											  int klen, bool enc);

/*
 * Return a newly created cipher context.  'cipher' specifies cipher algorithm
 * by identifier like PG_CIPHER_XXX.
 */
PgCipherCtx *
pg_cipher_ctx_create(int cipher, unsigned char *key, int klen, bool enc)
{
	PgCipherCtx *ctx = NULL;

	if (cipher > PG_MAX_CIPHER_ID)
		return NULL;

	ctx = ossl_cipher_ctx_create(cipher, key, klen, enc);

	return ctx;
}

void
pg_cipher_ctx_free(PgCipherCtx *ctx)
{
	EVP_CIPHER_CTX_free(ctx);
}

int
pg_cipher_blocksize(PgCipherCtx *ctx)
{
	Assert(ctx);

	return EVP_CIPHER_CTX_block_size(ctx);
}

/*
 * Encryption routine to encrypt data provided.
 *
 * ctx is the encryption context which must have been created previously.
 *
 * plaintext is the data we are going to encrypt
 * inlen is the length of the data to encrypt
 *
 * ciphertext is the encrypted result
 * outlen is the encrypted length
 *
 * iv is the IV to use.
 * ivlen is the IV length to use.
 *
 * outtag is the resulting tag.
 * taglen is the length of the tag.
 */
bool
pg_cipher_encrypt(PgCipherCtx *ctx, int cipher,
				  const unsigned char *plaintext, const int inlen,
				  unsigned char *ciphertext, int *outlen,
				  const unsigned char *iv, const int ivlen,
				  unsigned char *outtag, const int taglen)
{
	int			len;
	int			enclen;

	Assert(ctx != NULL);

	/*
	 * Here we are setting the IV for the context which was passed in.  Note
	 * that we signal to OpenSSL that we are configuring a new value for the
	 * context by passing in 'NULL' for the 2nd ('type') parameter.
	 */

	/*
	 * We don't use GCM mode, but it has a MAC, so we support it and test it
	 * in case we need it later.  XXX is this correct for GCM and CTR?
	 */
	/* Set the GCM IV length first */
	if (cipher == PG_CIPHER_AES_GCM &&
		!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, ivlen, NULL))
		return false;

	/* Set the IV for this encryption. */
	if (!EVP_EncryptInit_ex(ctx, NULL, NULL, NULL, iv))
		return false;

	/*
	 * This is the function which is actually performing the encryption for
	 * us.
	 */
	if (!EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, inlen))
		return false;

	enclen = len;

	/* Finalize the encryption, which could add more to output. */
	if (!EVP_EncryptFinal_ex(ctx, ciphertext + enclen, &len))
		return false;

	*outlen = enclen + len;

	/*
	 * Once all of the encryption has been completed we grab the tag.
	 */
	if (cipher == PG_CIPHER_AES_GCM &&
		!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, taglen, outtag))
		return false;

	return true;
}

/*
 * Decryption routine
 *
 * ctx is the encryption context which must have been created previously.
 *
 * ciphertext is the data we are going to decrypt
 * inlen is the length of the data to decrypt
 *
 * plaintext is the decrypted result
 * outlen is the decrypted length
 *
 * iv is the IV to use.
 * ivlen is the length of the IV.
 *
 * intag is the tag to use to verify.
 * taglen is the length of the tag.
 */
bool
pg_cipher_decrypt(PgCipherCtx *ctx, const int cipher,
				  const unsigned char *ciphertext, const int inlen,
				  unsigned char *plaintext, int *outlen,
				  const unsigned char *iv, const int ivlen,
				  unsigned char *intag, const int taglen)
{
	int			declen;
	int			len;

	/*
	 * Here we are setting the IV for the context which was passed in.  Note
	 * that we signal to OpenSSL that we are configuring a new value for the
	 * context by passing in 'NULL' for the 2nd ('type') parameter.
	 */

	/* XXX is this correct for GCM and CTR? */
	/* Set the GCM IV length first */
	if (cipher == PG_CIPHER_AES_GCM &&
		!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_IVLEN, ivlen, NULL))
		return false;

	/* Set the IV for this decryption. */
	if (!EVP_DecryptInit_ex(ctx, NULL, NULL, NULL, iv))
		return false;

	/*
	 * This is the function which is actually performing the decryption for
	 * us.
	 */
	if (!EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext, inlen))
		return false;

	declen = len;

	/* Set the expected tag value. */
	if (cipher == PG_CIPHER_AES_GCM &&
		!EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, taglen, intag))
		return false;

	/*
	 * Finalize the decryption, which could add more to output, this is also
	 * the step which checks the tag and we MUST fail if this indicates an
	 * invalid tag!
	 */
	if (!EVP_DecryptFinal_ex(ctx, plaintext + declen, &len))
		return false;

	*outlen = declen + len;

	return true;
}

/*
 * Routine to perform key wrapping for data provided.
 *
 * ctx is the encryption context which must have been created previously.
 *
 * plaintext is the data/key we are going to encrypt/wrap
 * inlen is the length of the data
 *
 * ciphertext is the wrapped result
 * outlen is the encrypted length (will be larger than input!)
 */
bool
pg_cipher_keywrap(PgCipherCtx *ctx,
				  const unsigned char *plaintext, const int inlen,
				  unsigned char *ciphertext, int *outlen)
{
	int			len;
	int			enclen;

	Assert(ctx != NULL);

	/*
	 * This is the function which is actually performing the encryption for
	 * us.
	 */
	if (!EVP_EncryptUpdate(ctx, ciphertext, &len, plaintext, inlen))
		return false;

	enclen = len;

	/* Finalize the encryption, which could add more to output. */
	if (!EVP_EncryptFinal_ex(ctx, ciphertext + enclen, &len))
		return false;

	*outlen = enclen + len;

	return true;
}

/*
 * Routine to perform key unwrapping of the data provided.
 *
 * ctx is the encryption context which must have been created previously.
 *
 * ciphertext is the wrapped key we are going to unwrap
 * inlen is the length of the data to decrypt/unwrap
 *
 * plaintext is the decrypted result
 * outlen is the decrypted length (will be smaller than input!)
 */
bool
pg_cipher_keyunwrap(PgCipherCtx *ctx,
					const unsigned char *ciphertext, const int inlen,
					unsigned char *plaintext, int *outlen)
{
	int			declen;
	int			len;

	/*
	 * This is the function which is actually performing the decryption for
	 * us.
	 */
	if (!EVP_DecryptUpdate(ctx, plaintext, &len, ciphertext, inlen))
		return false;

	declen = len;

	/*
	 * Finalize the decryption, which could add more to output, this is also
	 * the step which checks the tag and we MUST fail if this indicates an
	 * invalid result!
	 */
	if (!EVP_DecryptFinal_ex(ctx, plaintext + declen, &len))
		return false;

	*outlen = declen + len;

	return true;
}

/*
 * Returns the correct GCM cipher functions for OpenSSL based
 * on the key length requested.
 */
static ossl_EVP_cipher_func
get_evp_aes_gcm(int klen)
{
	switch (klen)
	{
		case PG_AES128_KEY_LEN:
			return EVP_aes_128_gcm;
		case PG_AES192_KEY_LEN:
			return EVP_aes_192_gcm;
		case PG_AES256_KEY_LEN:
			return EVP_aes_256_gcm;
		default:
			return NULL;
	}
}

/*
 * Returns the correct KWP cipher functions for OpenSSL based
 * on the key length requested.
 */
static ossl_EVP_cipher_func
get_evp_aes_kwp(int klen)
{
	switch (klen)
	{
		case PG_AES128_KEY_LEN:
			return EVP_aes_128_wrap_pad;
		case PG_AES192_KEY_LEN:
			return EVP_aes_192_wrap_pad;
		case PG_AES256_KEY_LEN:
			return EVP_aes_256_wrap_pad;
		default:
			return NULL;
	}
}

/*
 * Returns the correct CTR cipher functions for OpenSSL based
 * on the key length requested.
 */
static ossl_EVP_cipher_func
get_evp_aes_ctr(int klen)
{
	switch (klen)
	{
		case PG_AES128_KEY_LEN:
			return EVP_aes_128_ctr;
		case PG_AES192_KEY_LEN:
			return EVP_aes_192_ctr;
		case PG_AES256_KEY_LEN:
			return EVP_aes_256_ctr;
		default:
			return NULL;
	}
}

/*
 * Initialize and return an EVP_CIPHER_CTX. Returns NULL if the given
 * cipher algorithm is not supported or on failure.
 */
static EVP_CIPHER_CTX *
ossl_cipher_ctx_create(int cipher, unsigned char *key, int klen, bool enc)
{
	EVP_CIPHER_CTX *ctx;
	ossl_EVP_cipher_func func;
	int			ret;

	ctx = EVP_CIPHER_CTX_new();

	/*
	 * We currently only support AES GCM but others could be added in the
	 * future.
	 */
	switch (cipher)
	{
		case PG_CIPHER_AES_GCM:
			func = get_evp_aes_gcm(klen);
			if (!func)
				goto failed;
			break;
		case PG_CIPHER_AES_KWP:
			func = get_evp_aes_kwp(klen);

			/*
			 * Since wrapping will produce more output then input, and we have
			 * to be ready for that, OpenSSL requires that we explicitly
			 * enable wrapping for the context.
			 */
			EVP_CIPHER_CTX_set_flags(ctx, EVP_CIPHER_CTX_FLAG_WRAP_ALLOW);
			if (!func)
				goto failed;
			break;
		case PG_CIPHER_AES_CTR:
			func = get_evp_aes_ctr(klen);
			if (!func)
				goto failed;
			break;
		default:
			goto failed;
	}

	/*
	 * We create the context here based on the cipher requested and the
	 * provided key.  Note that the IV will be provided in the actual
	 * encryption call through another EVP_EncryptInit_ex call- this is fine
	 * as long as 'type' is passed in as NULL!
	 */
	if (enc)
		ret = EVP_EncryptInit_ex(ctx, (const EVP_CIPHER *) func(), NULL, key, NULL);
	else
		ret = EVP_DecryptInit_ex(ctx, (const EVP_CIPHER *) func(), NULL, key, NULL);

	if (!ret)
		goto failed;

	/* Set the key length based on the key length requested. */
	if (!EVP_CIPHER_CTX_set_key_length(ctx, klen))
		goto failed;

	return ctx;

failed:
	EVP_CIPHER_CTX_free(ctx);
	return NULL;
}
