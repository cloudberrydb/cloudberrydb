#ifndef DATALAKE_PROVIDERWRAPPER_H
#define DATALAKE_PROVIDERWRAPPER_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

struct ProviderInternalWrapper;

typedef struct ProviderInternalWrapper *providerWrapper;

providerWrapper initProvider(const char *type, bool readFdw, bool vectorization);

void createHandler(providerWrapper provider, void* sstate);

int64_t readFromProvider(providerWrapper provider, void *values, void *nulls);

int64_t readRecordBatch(providerWrapper provider, void** recordBatch);

int64_t readBufferFromProvider(providerWrapper provider, void* buffer, int64_t length);

int64_t writeToProvider(providerWrapper provider, const void* buf, int64_t length);

void destroyHandler(providerWrapper provider);

void destroyProvider(providerWrapper provider);

#ifdef __cplusplus
}
#endif

#endif
