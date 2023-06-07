#include "postgres.h"
#include "px.h"
#include "sm3.h"
#include "sm4.h"
#include "utils/memutils.h"

#ifdef OPENSSL_ALLOW_REDIRECT

static PX_Alias ossl_redirect_aliases[] = {
	{"sm4-ecb", "sm4-128-ecb"},
	{"sm4-cbc", "sm4-128-cbc"},
	{NULL},
};


bool px_find_digest_support_redirect(const char *name)
{
    return pg_strcasecmp("sm3", name) == 0;
}

int px_find_digest_redirect(const char *name, PX_MD **res) {
    PX_MD	   *h;
    if (pg_strcasecmp("sm3", name) == 0) {
		h = palloc(sizeof(*h));
		init_sm3(h);

		*res = h;
        return 0;
	}
    return PXE_NO_HASH;
}

bool px_find_cipher_support_redirect(const char *name) {
    name = px_resolve_alias(ossl_redirect_aliases, name);
    if (strcmp("sm4-128-cbc", name) == 0) {
        return true;
    }

    if (strcmp("sm4-128-cbc", name) == 0) {
        return true;
    }
    return false;
}

int px_find_cipher_redirect(const char *name, PX_Cipher **res) {
    PX_Cipher  *c = NULL;
    name = px_resolve_alias(ossl_redirect_aliases, name);
    if (strcmp("sm4-128-cbc", name) == 0) {
		c = sm4_load(MODE_CBC);
	} 

	if (strcmp("sm4-128-ecb", name) == 0) {
		c = sm4_load(MODE_ECB);
	}

    if (c == NULL)
		return PXE_NO_CIPHER;

	*res = c;
    return 0;
}

#endif // OPENSSL_ALLOW_REDIRECT