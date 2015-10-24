#define SHA256_PREFIX "sha256"

#define SHA256_PASSWD_LEN strlen(SHA256_PREFIX) + 64

#define isSHA256(passwd) \
	(strncmp(passwd, SHA256_PREFIX, strlen(SHA256_PREFIX)) == 0)

extern bool pg_sha256_encrypt(const char *pass, char *salt, size_t salt_len,
							  char *cryptpass);
