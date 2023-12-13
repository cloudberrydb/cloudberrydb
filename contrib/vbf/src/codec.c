#include <string.h>
#include <stdlib.h>

#ifdef ZSTD_CODEC
#include <zstd.h>
#include <zstd_errors.h>
#endif
#ifdef DEFLATE_CODEC
#include <zlib.h>
#endif

#include "vbf_private.h"
#include "vbf/errors.h"
#include "vbf/allocation.h"
#include "vbf/codec.h"

typedef void *(*compress_constructor_t) (int);
typedef void (*compress_destructor_t) (void *);
typedef int (*compress_t) (void *, void *, int, void *, int, int *);
typedef int (*decompress_t) (void *, void *, int, void *, int, int *);

typedef struct compress_handler {
	compress_constructor_t constructor;
	compress_destructor_t  destructor;
	compress_t           compress;
	decompress_t         decompress;
} compress_handler_t;

typedef struct compress_context {
	compress_handler_t *handler;
	void               *state;
} compress_context_t;

#ifdef ZSTD_CODEC
typedef struct zstd_state {
	int           level;
	ZSTD_CCtx    *cctx;
	ZSTD_DCtx    *dctx;
} zstd_state_t;

static void zstd_destructor(void *state)
{
	zstd_state_t *zstd_state = (zstd_state_t*) state;

	if (zstd_state->cctx)
		ZSTD_freeCCtx(zstd_state->cctx);

	if (zstd_state->dctx)
		ZSTD_freeDCtx(zstd_state->dctx);

	vbf_free(state);
}

static void *zstd_constructor(int compress_level)
{
	zstd_state_t *state = (zstd_state_t *) vbf_malloc(sizeof(zstd_state_t));
	if (state == NULL) {
		goto error;
	}

	memset(state, 0, sizeof(zstd_state_t));

	if (compress_level == 0) {
		compress_level = 1;
	}
	state->level = compress_level;

	state->cctx = ZSTD_createCCtx();
	if (state->cctx == NULL) {
		goto error;
	}

	state->dctx = ZSTD_createDCtx();
	if (!state->dctx) {
		goto error;
	}

	return state;

error:
	if (state) {
		zstd_destructor(state);
	}
	vbf_set_error("out of memroy");
	return NULL;
}

static int zstd_compress(void *state,
						 void *source_data,
						 int source_length,
						 void *compressed_buffer,
						 int compressed_buffer_length,
						 int *compressed_length)
{
	unsigned long  dst_length_used;
	zstd_state_t  *zstd_state = (zstd_state_t *) state;

	dst_length_used = ZSTD_compressCCtx(zstd_state->cctx,
										compressed_buffer,
										compressed_buffer_length,
										source_data,
										source_length,
										zstd_state->level);

	if (ZSTD_isError(dst_length_used))
	{
		if (ZSTD_getErrorCode(dst_length_used) != ZSTD_error_dstSize_tooSmall) {
			vbf_set_error("failed to compress block: %s", ZSTD_getErrorName(dst_length_used));
			return -1;
		}

		/*
		 * This error is returned when "compressed" output is bigger than
		 * uncompressed input. The caller can detect this by checking
		 * dst_used >= src_size
		 */
		dst_length_used = source_length;
	}

	*compressed_length = (int) dst_length_used;
	return 0;
}

static int zstd_decompress(void *state,
						   void *source_data,
						   int source_length,
						   void *uncompressed_buffer,
						   int uncompressed_buffer_length,
						   int *uncompressed_length)
{
	unsigned long  dst_length_used;
	zstd_state_t  *zstd_state = (zstd_state_t *) state;

	dst_length_used = ZSTD_decompressDCtx(zstd_state->dctx,
										  uncompressed_buffer,
										  uncompressed_buffer_length,
										  source_data,
										  source_length);

	if (ZSTD_isError(dst_length_used)) {
		vbf_set_error("failed to decompress block: %s", ZSTD_getErrorName(dst_length_used));
		return -1;
	}

	*uncompressed_length = (int) dst_length_used;
	return 0;
}

static compress_handler_t zstd_handler = {
	zstd_constructor,
	zstd_destructor,
	zstd_compress,
	zstd_decompress
};
#endif

#ifdef DEFLATE_CODEC
typedef struct zlib_state {
	int           level;
} zlib_state_t;

static void *zlib_constructor(int compress_level)
{
	zlib_state_t *state = (zlib_state_t *) vbf_malloc(sizeof(zlib_state_t));
	if (state == NULL) {
		vbf_set_error("out of memroy");
		return NULL;
	}

	memset(state, 0, sizeof(zlib_state_t));

	if (compress_level == 0) {
		compress_level = 1;
	}
	state->level = compress_level;

	return state;
}

static void zlib_destructor(void *state)
{
	vbf_free(state);
}

static int zlib_compress(void *state,
						 void *source_data,
						 int source_length,
						 void *compressed_buffer,
						 int compressed_buffer_length,
						 int *compressed_length)
{
	int            last_error;
	zlib_state_t  *zlib_state = (zlib_state_t *) state;
	unsigned long  amount_available_used = compressed_buffer_length;

	last_error = compress2((unsigned char *) compressed_buffer,
						   &amount_available_used,
						   source_data,
						   source_length,
						   zlib_state->level);

	*compressed_length = amount_available_used;

	if (last_error != Z_OK)
	{
		switch (last_error)
		{
			case Z_MEM_ERROR:
				vbf_set_error("failed to compress block: out of memory");
				return -1;
			case Z_BUF_ERROR:
				/*
				 * zlib returns this when it couldn't compress the data
				 * to a size smaller than the input.
				 *
				 * The caller expects to detect this themselves so we set
				 * dst_used accordingly.
				 */
				*compressed_length = source_length;
				break;

			default:
				/* shouldn't get here */
				vbf_set_error("zlib compression failed with error %d", last_error);
				return -1;
		}
	}

	return 0;
}

static int zlib_decompress(void *state,
						   void *source_data,
						   int source_length,
						   void *uncompressed_buffer,
						   int uncompressed_buffer_length,
						   int *uncompressed_length)
{
	int            last_error;
	unsigned long  amount_available_used = uncompressed_buffer_length;

	VBF_UNUSED(state);
	last_error = uncompress(uncompressed_buffer,
							&amount_available_used,
							(const Bytef *) source_data,
							source_length);

	*uncompressed_length = amount_available_used;

	if (last_error != Z_OK)
	{
		switch (last_error)
		{
			case Z_MEM_ERROR:
				vbf_set_error("failed to compress block: out of memory");
				return -1;
			case Z_BUF_ERROR:
				/*
				 * This would be a bug. We should have given a buffer big
				 * enough in the decompress case.
				 */
				vbf_set_error("buffer size %d insufficient for compressed data", uncompressed_buffer_length);
				return -1;

			case Z_DATA_ERROR:
				/*
				 * zlib data structures corrupted.
				 *
				 * Check out the error message: kind of like 'catalog
				 * convergence' for data corruption :-).
				 */
				vbf_set_error("zlib encountered data in an unexpected format");
				return -1;

			default:
				/* shouldn't get here */
				vbf_set_error("zlib decompression failed with error %d", last_error);
				return -1;
		}
	}

	return 0;
}

static compress_handler_t zlib_handler = {
	zlib_constructor,
	zlib_destructor,
	zlib_compress,
	zlib_decompress
};
#endif

static compress_handler_t none_handler = {
	NULL,
	NULL,
	NULL,
	NULL
};

static compress_handler_t *get_codec_handler(const char *compress_type, codec_type_t *type)
{
#ifdef ZSTD_CODEC
	if (strcasecmp("zstd", compress_type) == 0) {
		*type = CodeyZstd;
		return &zstd_handler;
	}
#endif

#ifdef DEFLATE_CODEC
	if (strcasecmp("zlib", compress_type) == 0) {
		*type = CodecZlib;
		return &zlib_handler;
	}
#endif

	if (strcasecmp("none", compress_type) == 0) {
		*type = CodecNULL;
		return &none_handler;
	}

	vbf_set_error("Unknown codec %s", compress_type);
	return NULL;
}

int vbf_codec_init(vbf_codec_t *codec,
				   const char *compress_type,
				   int compress_level)
{
	compress_context_t *context;
	compress_handler_t *compress_handler;

	context = vbf_malloc(sizeof(compress_context_t));
	if (context == NULL) {
		vbf_set_error("out of memory");
		return -1;
	}
	memset(context, 0, sizeof(compress_context_t));
	codec->codec_data = context;

	compress_handler = get_codec_handler(compress_type, &codec->codec_type);
	if (compress_handler == NULL) {
		return -1;
	}
	context->handler = compress_handler;

	if (compress_handler->constructor) {
		context->state = compress_handler->constructor(compress_level);
		if (context->state == NULL) {
			return -1;
		}
	}

	return 0;
}

void vbf_codec_fini(vbf_codec_t *codec)
{
	compress_context_t *context = (compress_context_t *) codec->codec_data;

	if (context != NULL) {
		if (context->handler->destructor && context->state) {
			context->handler->destructor(context->state);
		}

		vbf_free(context);
	}
}

int vbf_codec_encode(vbf_codec_t *codec,
					 void *source_data,
					 int source_length,
					 void *compressed_buffer,
					 int compressed_buffer_length,
					 int *compressed_length)
{
	compress_context_t *context = (compress_context_t *) codec->codec_data;

	return context->handler->compress(context->state,
									  source_data,
									  source_length,
									  compressed_buffer,
									  compressed_buffer_length,
									  compressed_length);
}

int vbf_codec_decode(vbf_codec_t *codec,
					 void *source_data,
					 int source_length,
					 void *uncompressed_buffer,
					 int uncompressed_buffer_length)
{
	int result;
	int uncompressed_length;
	compress_context_t *context = (compress_context_t *) codec->codec_data;

	result = context->handler->decompress(context->state,
										  source_data,
										  source_length,
										  uncompressed_buffer,
										  uncompressed_buffer_length,
										  &uncompressed_length);

	if (result == -1) {
		return -1;
	}

	if (uncompressed_length != uncompressed_buffer_length) {
		vbf_set_error("decompress returned length %d which is different than the "
					  "expected length %d", uncompressed_length, uncompressed_buffer_length);
		return -1;
	}

	return 0;
}
