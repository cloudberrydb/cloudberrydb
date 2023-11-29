#include "providerWrapper.h"
#include "provider.h"
#include <exception>

extern "C" {
#include "utils/elog.h"
}

struct ProviderInternalWrapper {
public:
	ProviderInternalWrapper(const char *type, bool readFdw, bool vectorization) {
		context = getProvider(type, readFdw, vectorization);
	}

	~ProviderInternalWrapper() {

	}

	Provider *getContext() {
		return context.get();
	}

private:
	std::shared_ptr<Provider> context;
};

#ifdef __cplusplus
extern "C" {
#endif

providerWrapper initProvider(const char *type, bool readFdw, bool vectorization) {
	ProviderInternalWrapper *prov;
	try
	{
		prov = new ProviderInternalWrapper(type, readFdw, vectorization);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "External table init provider failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "External table init provider failed.");
	}
	return prov;
}


void createHandler(providerWrapper provider, void* sstate) {
	try
	{
		provider->getContext()->createHandler(sstate);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "External table create handle failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "External table create handle failed.");
	}
	return;
}

int64_t readFromProvider(providerWrapper provider, void *values, void *nulls) {
	int64_t res = 0;
	try
	{
		res = provider->getContext()->read(values, nulls);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "External table read from oss failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "External table read from oss failed.");
	}
	return res;
}

int64_t readRecordBatch(providerWrapper provider, void** recordBatch)
{
	int64_t res = 0;
	try
	{
		res = provider->getContext()->read(recordBatch);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "External table readRecordBatch from oss failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "External table readRecordBatch from oss failed.");
	}
	return res;
}

int64_t readBufferFromProvider(providerWrapper provider, void* buffer, int64_t length)
{
	int64_t res = 0;
	try
	{
		res = provider->getContext()->readWithBuffer(buffer, length);
	}
	catch(const std::exception& e)
	{
		elog(ERROR, "External table read buffer from oss failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "External table read buffer from oss failed.");
	}
	return res;
}

int64_t writeToProvider(providerWrapper provider, const void* buf, int64_t length) {
	int64_t writenLen = 0;
	try
	{
		writenLen = provider->getContext()->write(buf, length);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "External table write to oss failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "External table write to oss failed.");
	}
	return writenLen;
}

void destroyHandler(providerWrapper provider) {
	try
	{
		provider->getContext()->destroyHandler();
	}
	catch(std::exception &e)
	{
		elog(ERROR, "External table destroy handle failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "External table destroy handle failed.");
	}
	return;
}

void destroyProvider(providerWrapper provider) {
	try
	{
		if (provider)
		{
			delete provider;
			provider = NULL;
		}
	}
	catch (std::exception &e)
	{
		elog(ERROR, "External table destroy provider handle failed, failed msg : %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "External table destroy provider handle failed.");
	}
	return;
}

#ifdef __cplusplus
}
#endif
