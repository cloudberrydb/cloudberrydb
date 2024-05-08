#ifndef HMSCLIENT_H
#define HMSCLIENT_H

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <stdint.h>

typedef struct HmsHandle HmsHandle;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

int HmsInitialize(const char *libJvm, const char *classPath, char *errMessage);
void HmsDestroy(void);
HmsHandle *HmsCreateHandle(char *errMessage);
void HmsDestroyHandle(HmsHandle *handle);

const char *HmsGetError(HmsHandle *handle);
bool HmsOpenConnection(HmsHandle *handle, const char *uris, bool debug);
bool HmsOpenConnectionWithKerberos(HmsHandle *handle, const char *uris,
		const char *servicePrincipal,
		const char *clientPrincipal,
		const char *clientKeytab,
		const char *rpcProtection,
		bool debug);
void HmsCloseConnection(HmsHandle *handle);

int HmsGetTableMetaData(HmsHandle *handle,
						const char *dbName,
						const char *tableName);

void HmsReleaseTableMetaData(HmsHandle *handle);

char **HmsPartTableGetKeys(HmsHandle *handle);
char *HmsTableGetField(HmsHandle *handle);
char **HmsGetTables(HmsHandle *handle, const char *dbName);
char *HmsTableGetFormat(HmsHandle *handle);
bool HmsTableIsPartitionTable(HmsHandle *handle);
char *HmsTableGetTableType(HmsHandle *handle);
char *HmsTableGetSerialLib(HmsHandle *handle);
char *HmsTableGetParameters(HmsHandle *handle);
char *HmsTableGetLocation(HmsHandle *handle);
bool HmsTableIsTransactionalTable(HmsHandle *handle);
char **HmsTableGetColumnComments(HmsHandle *handle);

#ifdef __cplusplus
} // extern "C"
#endif // __cpluscplus

#endif // HMSCLIENT_H
