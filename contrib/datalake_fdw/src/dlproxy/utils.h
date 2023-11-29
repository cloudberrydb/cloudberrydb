#ifndef _UTILS_H_
#define _UTILS_H_

#include "postgres.h"

/* convert input string to upper case and prepend "X-GP-OPTIONS-" prefix */
char	   *normalize_key_name(const char *key);

/* Concatenate multiple literal strings using stringinfo */
char	   *concat(int num_args,...);

/* Get authority (host:port) for the dlproxy server URL */
char	   *get_authority(void);

/* get the name of the type, given the OID */
char	   *TypeOidGetTypename(Oid typid);

/* Returns the dlproxy Host defined in the DLPROXY_HOST
 * environment variable or the default when undefined
 */
const char *get_dlproxy_host(void);

/* Returns the dlproxy Port defined in the DLPROXY_PORT
 * environment variable or the default when undefined
 */
const int  get_dlproxy_port(void);

/* Returns the namespace (schema) name for a given namespace oid */
char	   *GetNamespaceName(Oid nsp_oid);

#define DLPROXY_PROFILE       "PROFILE"
#define ACCESSOR              "ACCESSOR"
#define ENV_DLPROXY_HOST      "DLPROXY_HOST"
#define ENV_DLPROXY_PORT      "DLPROXY_PORT"
#define DLPROXY_DEFAULT_HOST  "localhost"
#define DLPROXY_DEFAULT_PORT  5888

#endif							/* _UTILS_H_ */
