#ifndef VBF_CODEC_H
#define VBF_CODEC_H
#ifdef __cplusplus
extern "C" {
#define CLOSE_EXTERN }
#else
#define CLOSE_EXTERN
#endif

typedef enum codec_type {
	CodecNULL,
	CodecZlib,
	CodeyZstd
} codec_type_t;

typedef struct vbf_codec {
	codec_type_t  codec_type;
	void         *codec_data;
} vbf_codec_t;

int vbf_codec_init(vbf_codec_t *codec,
				   const char *compress_type,
				   int compress_level);
void vbf_codec_fini(vbf_codec_t *codec);
int vbf_codec_encode(vbf_codec_t *codec,
					 void *source_data,
					 int source_length,
					 void *compressed_buffer,
					 int compressed_buffer_length,
					 int *compressed_length);
int vbf_codec_decode(vbf_codec_t *codec,
					 void *source_data,
					 int source_length,
					 void *uncompressed_buffer,
					 int uncompressed_buffer_length);

CLOSE_EXTERN
#endif
