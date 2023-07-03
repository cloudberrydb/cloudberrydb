/*-------------------------------------------------------------------------
 *
 * testcrypto.c
 *    A utility to test our encryption / decryption routines.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/test/crypto/testcrypto.c
 *
 *-------------------------------------------------------------------------
 */

#define FRONTEND 1

#define EXITSUCCESS 0
#define EXITDECRYPTFAIL 1
#define EXITFAILURE 2

#include "postgres_fe.h"

#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>

//#include "common/hex.h"
#include "common/cipher.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "pg_getopt.h"

static const char *progname;

static void
usage(const char *progname)
{
	printf(_("%s tests encryption/decryption routines in PG common library.\n\n"), progname);
	printf(_("Usage:\n"));
	printf(_("  %s [OPTION]\n"), progname);
	printf("\n");
	printf(_("  Performs one encryption and one decryption run.\n"));
	printf("\n");
	printf(_("  Encrypts the provided plaintext (or the empty string if none given) and generates a tag, if using AES-GCM, using the key and IV given.\n"));
	printf(_("  After encryption, compares provided ciphertext to resulting ciphertext.\n"));
	printf(_("  Compares provided tag, if any, to resulting tag.\n"));
	printf(_("  If no tag is provided, then the tag created during encryption is used during decryption.\n"));
	printf("\n");
	printf(_("  Decrypts the provided ciphertext (or the empty string if none given) using the key, and IV + tag given if using AES-GCM.\n"));
	printf(_("  After successful decryption (requires tag to match for AES-GCM), compares provided plaintext to resulting plaintext.\n"));
	printf(_("  Exits with '1' if decryption fails.\n"));
	printf("\n");
	printf(_("  Exits with '2' for any other failure.\n"));
	printf("\n");
	printf(_("  Key is always required, IV is required for AES-GCM mode.\n"));
	printf("\n");
	printf(_("  Algorithms supported are AES-GCM and AES-KWP.\n"));
	printf("\n");
	printf(_("\nOptions:\n"));
	printf(_("  -a, --algorithm=ALG    Crypto algorithm to use\n"));
	printf(_("  -i, --init-vector=IV   Initialization vector to use\n"));
	printf(_("  -k, --key=KEY          Key to use, in hex\n"));
	printf(_("  -p, --plain-text=PT    Plain text to encrypt\n"));
	printf(_("  -c, --cipher-text=CT   Cipher text to decrypt\n"));
	printf(_("  -t, --tag=TAG          Tag to use for decryption\n"));
	printf(_("  -T, --tag-length=LEN   Length of tag to use for encryption\n"));
	printf(_("  -v, --verbose          verbose output\n"));
	printf(_("  -V, --version          output version information, then exit\n"));
	printf(_("  -?, --help             show this help, then exit\n"));
	printf("\n");
	printf(_("Report bugs to <%s>.\n"), PACKAGE_BUGREPORT);
}

/*
 * HEX
 */
static const char hextbl[] = "0123456789abcdef";

static const int8 hexlookup[128] = {
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	0, 1, 2, 3, 4, 5, 6, 7, 8, 9, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, 10, 11, 12, 13, 14, 15, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
};

static uint64
hex_encode(const char *src, size_t len, char *dst)
{
	const char *end = src + len;

	while (src < end)
	{
		*dst++ = hextbl[(*src >> 4) & 0xF];
		*dst++ = hextbl[*src & 0xF];
		src++;
	}
	return (uint64) len * 2;
}

static inline char
get_hex(const char *cp)
{
	unsigned char c = (unsigned char) *cp;
	int			res = -1;

	if (c < 127)
		res = hexlookup[c];

	if (res < 0)
		pg_log_fatal("invalid hexadecimal digit: \"%s\"",cp);

	return (char) res;
}

static uint64
hex_decode(const char *src, size_t len, char *dst)
{
	const char *s,
			   *srcend;
	char		v1,
				v2,
			   *p;

	srcend = src + len;
	s = src;
	p = dst;
	while (s < srcend)
	{
		if (*s == ' ' || *s == '\n' || *s == '\t' || *s == '\r')
		{
			s++;
			continue;
		}
		v1 = get_hex(s) << 4;
		s++;
		if (s >= srcend)
			pg_log_fatal("invalid hexadecimal data: odd number of digits");

		v2 = get_hex(s);
		s++;
		*p++ = v1 | v2;
	}

	return p - dst;
}

static uint64
pg_hex_enc_len(size_t srclen)
{
	return (uint64) srclen << 1;
}

static uint64
pg_hex_dec_len(size_t srclen)
{
	return (uint64) srclen >> 1;
}

int
main(int argc, char *argv[])
{
	char	   *algorithm = NULL,
			   *iv_hex = NULL,
			   *key_hex = NULL,
			   *plaintext_hex = NULL,
			   *ciphertext_hex = NULL,
			   *tag_hex = NULL;

	unsigned char *plaintext = NULL,
			   *ciphertext = NULL,
			   *key = NULL,
			   *iv = NULL,
			   *tag = NULL,
			   *tag_result = NULL,
			   *result = NULL;

	int			verbose = 0,
				plaintext_len = 0,
				ciphertext_len = 0,
				key_len = 0,
				iv_len = 0,
				tag_len = 16,
				result_len = 0,
				blocksize = 0,
				cipher = PG_CIPHER_AES_GCM;

	PgCipherCtx *ctx = NULL;

	static struct option long_options[] = {
		{"algorithm", required_argument, NULL, 'a'},
		{"init-vector", required_argument, NULL, 'i'},
		{"key", required_argument, NULL, 'k'},
		{"plain-text", required_argument, NULL, 'p'},
		{"cipher-text", required_argument, NULL, 'c'},
		{"tag", required_argument, NULL, 't'},
		{"tag-length", required_argument, NULL, 'T'},
		{"verbose", required_argument, NULL, 'v'},
		{NULL, 0, NULL, 0}
	};

	int			c;

	pg_logging_init(argv[0]);
	set_pglocale_pgservice(argv[0], PG_TEXTDOMAIN("testcrypto"));
	progname = get_progname(argv[0]);

	if (argc > 1)
	{
		if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0)
		{
			usage(progname);
			exit(EXITSUCCESS);
		}
		if (strcmp(argv[1], "--version") == 0 || strcmp(argv[1], "-V") == 0)
		{
			puts("testcrypto (PostgreSQL) " PG_VERSION);
			exit(EXITSUCCESS);
		}
	}

	/* Process command-line argument */

	while ((c = getopt_long(argc, argv, "a:i:k:p:c:t:T:v", long_options, NULL)) != -1)
	{
		switch (c)
		{
			case 'a':
				algorithm = pg_strdup(optarg);
				break;

			case 'i':
				iv_hex = pg_strdup(optarg);
				break;

			case 'k':
				key_hex = pg_strdup(optarg);
				break;

			case 'p':
				plaintext_hex = pg_strdup(optarg);
				break;

			case 'c':
				ciphertext_hex = pg_strdup(optarg);
				break;

			case 't':
				tag_hex = pg_strdup(optarg);
				break;

			case 'T':
				tag_len = atoi(optarg);
				break;

			case 'v':
				verbose = 1;
				break;

			default:
				fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
				exit(EXITFAILURE);
		}
	}

	/* Complain if any arguments remain */
	if (optind < argc)
	{
		pg_log_error("too many command-line arguments (first is \"%s\")",
					 argv[optind]);
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(EXITFAILURE);
	}

	/* Check options passed in */
	if (algorithm)
	{
		if (strcmp(algorithm, "AES-GCM") == 0)
			cipher = PG_CIPHER_AES_GCM;
		else if (strcmp(algorithm, "AES-KWP") == 0)
			cipher = PG_CIPHER_AES_KWP;
		else
		{
			pg_log_error("Unsupported algorithm selected.");
			fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
					progname);
			exit(EXITFAILURE);
		}
	}

	if (key_hex == NULL)
	{
		pg_log_error("Key must be provided");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(EXITFAILURE);
	}
	else
	{
		size_t		key_hex_len = strlen(key_hex);

		key_len = pg_hex_dec_len(key_hex_len);

		key = pg_malloc0(key_len);
		hex_decode(key_hex, key_hex_len, (char *) key);
	}

	if (cipher == PG_CIPHER_AES_GCM && iv_hex == NULL)
	{
		pg_log_error("Initialization vector must be provided");
		fprintf(stderr, _("Try \"%s --help\" for more information.\n"),
				progname);
		exit(EXITFAILURE);
	}
	else if (cipher == PG_CIPHER_AES_GCM)
	{
		size_t		iv_hex_len = strlen(iv_hex);

		iv_len = pg_hex_dec_len(iv_hex_len);

		iv = pg_malloc0(iv_len);
		hex_decode(iv_hex, iv_hex_len, (char *) iv);
	}

	if (plaintext_hex)
	{
		size_t		plaintext_hex_len = strlen(plaintext_hex);

		plaintext_len = pg_hex_dec_len(plaintext_hex_len);

		plaintext = pg_malloc0(plaintext_len);
		hex_decode(plaintext_hex, plaintext_hex_len, (char *) plaintext);
	}

	/*
	 * OpenSSL 1.1.1d and earlier crashes on some zero-length plaintext and
	 * ciphertext strings.  It crashes on an encryption call to
	 * EVP_EncryptFinal_ex(() in GCM mode of zero-length strings if plaintext
	 * is NULL, even though plaintext_len is zero.  Setting plaintext to
	 * non-NULL allows it to work.  In KWP mode, zero-length strings fail if
	 * plaintext_len = 0 and plaintext is non-NULL (the opposite).  OpenSSL
	 * 1.1.1e+ is fine with all options.
	 */
	else if (cipher == PG_CIPHER_AES_GCM)
	{
		plaintext_len = 0;
		plaintext = pg_malloc0(1);
	}

	if (ciphertext_hex)
	{
		size_t		ciphertext_hex_len = strlen(ciphertext_hex);

		ciphertext_len = pg_hex_dec_len(ciphertext_hex_len);

		ciphertext = pg_malloc0(ciphertext_len);
		hex_decode(ciphertext_hex, ciphertext_hex_len,
					  (char *) ciphertext);
	}
	/* see OpenSSL 1.1.1d item above, though crash only happens in GCM mode */
	else if (cipher == PG_CIPHER_AES_GCM)
	{
		ciphertext_len = 0;
		ciphertext = pg_malloc0(1);
	}

	if (cipher == PG_CIPHER_AES_GCM)
		tag_result = pg_malloc0(tag_len);

	if (tag_hex)
	{
		size_t		tag_hex_len = strlen(tag_hex);

		tag_len = pg_hex_dec_len(tag_hex_len);

		tag = pg_malloc0(tag_len);
		hex_decode(tag_hex, tag_hex_len, (char *) tag);
	}
	else
		tag = tag_result;

	if (verbose)
	{
		printf("Alrogithm: %d\n", cipher);
		printf("Key length: %d (%d bits)\n", key_len, key_len * 8);
		printf("IV length: %d (%d bits)\n", iv_len, iv_len * 8);
		printf("Tag length: %d (%d bits)\n", tag_len, tag_len * 8);
		printf("Plaintext length: %d\n", plaintext_len);
		printf("Ciphertext length: %d\n", ciphertext_len);
	}

	/*
	 * Encryption
	 *
	 * We run through the encryption even if there wasn't a plaintext
	 * provided- in that case we just encrypt the empty string.
	 */
	ctx = pg_cipher_ctx_create(cipher, key, key_len, true);
	if (!ctx)
	{
		pg_log_error("Error creating encryption context, be sure key is of supported length");
		exit(EXITFAILURE);
	}

	blocksize = pg_cipher_blocksize(ctx);

	/* If we were provided with a plaintext input */
	if (plaintext_len != 0)
	{
		/* Encryption might result in as much as input length + blocksize */
		result_len = plaintext_len + blocksize;
		result = palloc0(result_len);

		if (ciphertext_hex == NULL)
		{
			ciphertext = result;
			ciphertext_len = plaintext_len;
		}
	}

	if (cipher == PG_CIPHER_AES_GCM)
	{
		if (!pg_cipher_encrypt(ctx, cipher,
							   plaintext, plaintext_len,
							   result, &result_len,
							   iv, iv_len,
							   tag_result, tag_len))
		{
			pg_log_error("Error during encryption.");
			exit(EXITFAILURE);
		}
	}
	else if (cipher == PG_CIPHER_AES_KWP)
	{
		if (!pg_cipher_keywrap(ctx,
							   plaintext, plaintext_len,
							   result, &result_len))
		{
			pg_log_error("Error during encryption.");
			exit(EXITFAILURE);
		}
	}

	if (verbose || ciphertext == NULL)
	{
		uint64		result_hex_len = pg_hex_enc_len(result_len);
		char	   *result_hex = palloc0(result_hex_len + 1);

		hex_encode((char *) result, result_len, result_hex);
		result_hex[result_hex_len] = '\0';

		printf("ciphertext: %s\n", result_hex);

		if (cipher == PG_CIPHER_AES_GCM)
		{
			result_hex_len = pg_hex_enc_len(tag_len);
			result_hex = palloc0(result_hex_len + 1);

			hex_encode((char *) tag_result, tag_len, result_hex);
			result_hex[result_hex_len] = '\0';

			printf("tag: %s\n", result_hex);
		}
	}

	/*
	 * Report on non-matching results, but still go through the decryption
	 * routine to make sure that we get the correct result, and then error
	 * out.
	 */
	if (plaintext_len != 0 && ciphertext != NULL && memcmp(ciphertext, result, plaintext_len) != 0)
		pg_log_error("Provided ciphertext does not match");

	if (cipher == PG_CIPHER_AES_GCM && tag != tag_result && memcmp(tag, tag_result, tag_len) != 0)
		pg_log_error("Provided tag does not match");

	/*
	 * If a ciphertext was provided then use that as the max size of our
	 * plaintext result.  We shouldn't ever get a result larger.
	 */
	if (ciphertext_len != 0)
	{
		result_len = ciphertext_len;
		result = palloc0(result_len);
	}

	/*
	 * Decryption
	 *
	 * We run through the decryption even if there wasn't a ciphertext
	 * provided- in that case we just decrypt the empty string.
	 */
	ctx = pg_cipher_ctx_create(cipher, key, key_len, false);
	if (!ctx)
	{
		pg_log_error("Error creating decryption context, be sure key is of supported length");
		exit(EXITFAILURE);
	}

	if (cipher == PG_CIPHER_AES_GCM)
	{
		if (!pg_cipher_decrypt(ctx, cipher,
							   ciphertext, ciphertext_len,
							   result, &result_len,
							   iv, iv_len,
							   tag, tag_len))
		{
			pg_log_error("Error during decryption.");
			exit(EXITDECRYPTFAIL);
		}
	}
	else if (cipher == PG_CIPHER_AES_KWP)
	{
		if (!pg_cipher_keyunwrap(ctx,
								 ciphertext, ciphertext_len,
								 result, &result_len))
		{
			pg_log_error("Error during decryption.");
			exit(EXITDECRYPTFAIL);
		}
	}

	if (verbose || plaintext == NULL)
	{
		uint64		result_hex_len = pg_hex_enc_len(result_len);
		char	   *result_hex = palloc0(result_hex_len + 1);

		hex_encode((char *) result, result_len, result_hex);
		result_hex[result_hex_len] = '\0';

		printf("plaintext: %s\n", result_hex);
	}

	if (ciphertext_len != 0 && plaintext != NULL && memcmp(plaintext, result, plaintext_len) != 0)
	{
		pg_log_error("Provided plaintext does not match");
		exit(EXITFAILURE);
	}

	exit(EXITSUCCESS);
}
