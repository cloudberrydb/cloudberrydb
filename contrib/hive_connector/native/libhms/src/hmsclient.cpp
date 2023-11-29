
extern "C" {
#include "postgres.h"
#include "catalog/pg_type.h"
#include "utils/timestamp.h"
}

#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dlfcn.h>
#include <jni.h>
#include "hmsclient.h"

#define ERRMSGSIZE 1024

typedef jint ((*_JNI_CreateJavaVM_PTR) (JavaVM **vm, JNIEnv **env, void *vmArgs));

typedef struct HmsHandle
{
	jobject     objErrBuf;
	jobject     objValBuf;
	jobject     objHmsClient;
	jobject     objMetaData;
	char       *errMessage;
} HmsHandle;

static JavaVM     *jvm;
static JNIEnv     *jni;
static jclass      clsMsgBuf;
static jclass      clsHmsClient;
static jclass      clsTable;
static jmethodID   getVal;
static jmethodID   resetVal;
static jmethodID   openConnection;
static jmethodID   openConnectionWithKerberos;
static jmethodID   tableExists;
static jmethodID   closeConnection;

static jmethodID   consMsgBuf;
static jmethodID   consHmsClient;
static jmethodID   consTable;

static jmethodID   isPartitionTable;
static jmethodID   getPartKeys;
static jmethodID   getPartKeyTypes;
static jmethodID   getPartKeyValues;
static jmethodID   getPartValues;
static jmethodID   getLocations;
static jmethodID   getPartFields;
static jmethodID   getField;
static jmethodID   getTableMetaData;
static jmethodID   getTables;
static jmethodID   getFormat;
static jmethodID   getTableType;
static jmethodID   getSerialLib;
static jmethodID   getParameters;
static jmethodID   getPartitionNumber;
static jmethodID   getLocation;
static jmethodID   isTransactionalTable;

static void       *soHandle;

static void *
palloc0_wrapper(Size size)
{
	void *result = NULL;

	PG_TRY();
	{
		result = palloc0(size);
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();

	return result;
}

static char *
pstrdup_wrapper(const char *in)
{
	char *result = NULL;

	PG_TRY();
	{
		result = pstrdup(in);
	}
	PG_CATCH();
	{
	}
	PG_END_TRY();

	return result;
}

int
HmsInitialize(const char *libJvm, const char *classPath, char *errMessage)
{
	jint                   result;
	JavaVMInitArgs         vmArgs;
	JavaVMOption          *options;
	_JNI_CreateJavaVM_PTR  createJavaVM;
	const char            *errJvm;

	soHandle = dlopen(libJvm, RTLD_LAZY);
	if(soHandle == NULL)
	{
		sprintf(errMessage, "failed to load \"%s\": %s", libJvm, dlerror());
		return -1;
	}

	if (!jvm)
	{
		options = (JavaVMOption*) palloc0_wrapper(sizeof(JavaVMOption)*2);

		options[0].optionString = (char *) palloc0_wrapper(strlen(classPath) + 64);
		if (!options[0].optionString)
		{
			sprintf(errMessage, "failed to load \"%s\": out of memory", libJvm);
			return -1;
		}

		strcpy(options[0].optionString, "-Djava.class.path=");
		strcat(options[0].optionString, classPath);

		options[1].optionString = (char *) palloc0_wrapper(32);
		strcpy(options[1].optionString, "-Xrs");

		vmArgs.version = JNI_VERSION_1_6;
		vmArgs.nOptions = 2;
		vmArgs.options = options;
		vmArgs.ignoreUnrecognized = false;
		createJavaVM = (_JNI_CreateJavaVM_PTR) dlsym(soHandle, "JNI_CreateJavaVM");
		if (createJavaVM == NULL)
		{
			sprintf(errMessage, "failed to locate \"JNI_CreateJavaVM\": %s", dlerror());
			return -1;
		}

		result = createJavaVM(&jvm, &jni, &vmArgs);
		if (result != JNI_OK)
		{
			sprintf(errMessage, "failed to call JNI_CreateJavaVM: %d", result);
			return -1;
		}
	}

	clsMsgBuf = jni->FindClass("MessageBuffer");
	if (clsMsgBuf == NULL)
	{
		errJvm = "failed to locate class \"MessageBuffer\"";
		goto failed;
	}

	clsHmsClient = jni->FindClass("HMSClient");
	if (clsHmsClient == NULL)
	{
		errJvm = "failed to locate class \"HMSClient\"";
		goto failed;
	}

	clsTable = jni->FindClass("TableMetaData");
	if (clsTable == NULL)
	{
		errJvm = "failed to locate class \"TableMetaData\"";
		goto failed;
	}

	getVal = jni->GetMethodID(clsMsgBuf, "getVal", "()Ljava/lang/String;");
	if (getVal == NULL)
	{
		errJvm = "failed to locate \"MessageBuffer->getVal\"";
		goto failed;
	}

	resetVal = jni->GetMethodID(clsMsgBuf, "resetVal", "()V");
	if (resetVal == NULL)
	{
		errJvm = "failed to locate \"MessageBuffer->resetVal\"";
		goto failed;
	}

	openConnection = jni->GetMethodID(clsHmsClient, "openConnection",
				"(Ljava/lang/String;LMessageBuffer;I)I");
	if (openConnection == NULL)
	{
		errJvm = "failed to locate \"HMSClient->openConnection(String, MessageBuffer, int)\"";
		goto failed;
	}

	openConnectionWithKerberos = jni->GetMethodID(clsHmsClient, "openConnection",
				"(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;LMessageBuffer;I)I");
	if (openConnectionWithKerberos == NULL)
	{
		errJvm = "failed to locate \"HMSClient->openConnection(String, String, String, MessageBuffer, int)\"";
		goto failed;
	}

	tableExists = jni->GetMethodID(clsHmsClient, "tableExists",
			"(Ljava/lang/String;Ljava/lang/String;LMessageBuffer;LMessageBuffer;)I");
	if (tableExists == NULL)
	{
		errJvm = "failed to locate \"HMSClient->tableExists\"";
		goto failed;
	}

	closeConnection = jni->GetMethodID(clsHmsClient, "closeConnection", "()V");
	if (closeConnection == NULL)
	{
		errJvm = "failed to locate \"HMSClient->closeConnection\"";
		goto failed;
	}

	getTables = jni->GetMethodID(clsHmsClient, "getTables",
			"(Ljava/lang/String;LMessageBuffer;)[Ljava/lang/Object;");
	if (getTables == NULL)
	{
		errJvm = "failed to locate \"HMSClient->getTables\"";
		goto failed;
	}

	getPartFields = jni->GetMethodID(clsTable, "getPartFields", "()[Ljava/lang/Object;");
	if (getPartFields == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getPartFields\"";
		goto failed;
	}

	getField = jni->GetMethodID(clsTable, "getField", "()Ljava/lang/Object;");
	if (getField == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getField\"";
		goto failed;
	}

	getPartKeys = jni->GetMethodID(clsTable, "getPartKeys", "()[Ljava/lang/Object;");
	if (getPartKeys == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getPartKeys\"";
		goto failed;
	}

	getPartKeyTypes = jni->GetMethodID(clsTable, "getPartKeyTypes", "()[Ljava/lang/Object;");
	if (getPartKeyTypes == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getPartKeyTypes\"";
		goto failed;
	}

	getPartKeyValues = jni->GetMethodID(clsTable, "getPartKeyValues", "(I)[Ljava/lang/Object;");
	if (getPartKeyValues == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getPartKeyValues\"";
		goto failed;
	}

	getPartValues = jni->GetMethodID(clsTable, "getPartValues", "(I)[Ljava/lang/Object;");
	if (getPartValues == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getPartValues\"";
		goto failed;
	}

	getLocations = jni->GetMethodID(clsTable, "getLocations", "()[Ljava/lang/Object;");
	if (getLocations == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getLocations\"";
		goto failed;
	}

	getFormat = jni->GetMethodID(clsTable, "getFormat", "()Ljava/lang/Object;");
	if (getFormat == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getFormat\"";
		goto failed;
	}

	isPartitionTable = jni->GetMethodID(clsTable, "isPartitionTable", "()I");
	if (isPartitionTable == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->isPartitionTable\"";
		goto failed;
	}

	getTableType = jni->GetMethodID(clsTable, "getTableType", "()Ljava/lang/Object;");
	if (getTableType == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getTableType\"";
		goto failed;
	}

	getSerialLib = jni->GetMethodID(clsTable, "getSerialLib", "()Ljava/lang/Object;");
	if (getSerialLib == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getSerialLib\"";
		goto failed;
	}

	getParameters = jni->GetMethodID(clsTable, "getParameters", "()Ljava/lang/Object;");
	if (getParameters == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getParameters\"";
		goto failed;
	}

	getPartitionNumber = jni->GetMethodID(clsTable, "getPartitionNumber", "()I");
	if (getPartitionNumber == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getPartitionNumber\"";
		goto failed;
	}

	getLocation = jni->GetMethodID(clsTable, "getLocation", "()Ljava/lang/Object;");
	if (getLocation == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->getLocation\"";
		goto failed;
	}

	isTransactionalTable = jni->GetMethodID(clsTable, "isTransactionalTable", "()I");
	if (isTransactionalTable == NULL)
	{
		errJvm = "failed to locate \"TableMetaData->isTransactionalTable\"";
		goto failed;
	}

	getTableMetaData = jni->GetMethodID(clsHmsClient,
			"getTableMetaData",
			"(Ljava/lang/String;Ljava/lang/String;LTableMetaData;LMessageBuffer;)I");
	if (getTableMetaData == NULL)
	{
		errJvm = "failed to locate \"HMSClient->getTableMetaData\"";
		goto failed;
	}

	consMsgBuf = jni->GetMethodID(clsMsgBuf, "<init>", "()V");
	if (consMsgBuf == NULL)
	{
		errJvm = "failed to locate method \"MessageBuffer->init\"";
		goto failed;
	}

	consHmsClient = jni->GetMethodID(clsHmsClient, "<init>", "()V");
	if (consHmsClient == NULL)
	{
		errJvm = "failed to locate method \"HMSClient->init\"";
		goto failed;
	}

	consTable = jni->GetMethodID(clsHmsClient, "<init>", "()V");
	if (consTable == NULL)
	{
		errJvm = "failed to locate method \"TableMetaData->init\"";
		goto failed;
	}

	return 0;

failed:
	strcpy(errMessage, errJvm);
	return -1;
}

void
HmsDestroy(void)
{
	if (soHandle)
		dlclose(soHandle);

	if (jvm != NULL)
		jvm->DestroyJavaVM();
}

HmsHandle *
HmsCreateHandle(char *errMessage)
{
	const char            *errJvm;
	HmsHandle             *handle;

	handle = (HmsHandle *) palloc0_wrapper(sizeof(HmsHandle));
	if (!handle)
	{
		errJvm = "failed to create hms handle: out of memory";
		goto failed;
	}

	handle->objErrBuf = jni->NewObject(clsMsgBuf, consMsgBuf);
	if (handle->objErrBuf == NULL)
	{
		errJvm = "failed to create \"MessageBuffer\": out of memory";
		goto failed;
	}

	handle->objValBuf = jni->NewObject(clsMsgBuf, consMsgBuf);
	if (handle->objValBuf == NULL)
	{
		errJvm = "failed to create \"MessageBuffer\": out of memory";
		goto failed;
	}

	handle->objHmsClient = jni->NewObject(clsHmsClient, consHmsClient);
	if (handle->objHmsClient == NULL)
	{
		errJvm = "failed to create \"HMSClient\": out of memory";
		goto failed;
	}

	handle->errMessage = (char *) palloc0_wrapper(ERRMSGSIZE);
	if (!handle->errMessage)
	{
		errJvm = "failed to create \"HMSClient\": out of memory";
		goto failed;
	}
	return handle;

failed:
	strcpy(errMessage, errJvm);
	return NULL;
}

void
HmsDestroyHandle(HmsHandle *handle)
{
	if (handle->objErrBuf)
		jni->DeleteLocalRef(handle->objErrBuf);

	if (handle->objValBuf)
		jni->DeleteLocalRef(handle->objValBuf);

	if (handle->errMessage)
		pfree(handle->errMessage);

	HmsCloseConnection(handle);

	if (handle->objHmsClient)
		jni->DeleteLocalRef(handle->objHmsClient);

	pfree(handle);
}

const char *
HmsGetError(HmsHandle *handle)
{
	return handle->errMessage;
}

static jstring
createJString(const char *value)
{
	jstring result = NULL;

	try
	{
		result = jni->NewStringUTF(value);
	}
	catch (...)
	{
		/* out of memory */
	}

	return result;
}

static void
HmsProcessErrorMessage(HmsHandle *handle, const char *message)
{
	jstring jerrBuf;
	const char *errorMessage;

	if (message)
	{
		strncpy(handle->errMessage, message, ERRMSGSIZE - 1);
		return;
	}

	jerrBuf = (jstring) jni->CallObjectMethod(handle->objErrBuf, getVal);
	errorMessage = (const char *) jni->GetStringUTFChars(jerrBuf, NULL);
	strncpy(handle->errMessage, errorMessage, ERRMSGSIZE - 1);
	jni->ReleaseStringUTFChars(jerrBuf, errorMessage);
}

bool
HmsOpenConnection(HmsHandle *handle, const char *uris, bool debug)
{
	int result;
	const char *hmsUris = "";
	jstring jhmsUris;

	if (uris)
		hmsUris = uris;

	if ((jhmsUris = createJString(hmsUris)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return false;
	}

	jni->CallVoidMethod(handle->objErrBuf, resetVal);
	result = jni->CallIntMethod(handle->objHmsClient, openConnection,
							jhmsUris,
							handle->objErrBuf,
							debug ? 1 : 0);

	jni->DeleteLocalRef(jhmsUris);

	if (result < 0)
	{
		HmsProcessErrorMessage(handle, NULL);
		return false;
	}

	return true;
}

bool
HmsOpenConnectionWithKerberos(HmsHandle *handle, const char *uris,
		const char *servicePrincipal,
		const char *clientPrincipal,
		const char *clientKeytab,
		bool debug)
{
	int result;
	jstring jhmsUris;
	jstring jhmsServicePrincipal;
	jstring jhmsClientPrincipal;
	jstring jhmsClientKeytab;

	if ((jhmsUris = createJString(uris)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return false;
	}

	if ((jhmsServicePrincipal = createJString(servicePrincipal)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return false;
	}

	if ((jhmsClientPrincipal = createJString(clientPrincipal)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return false;
	}

	if ((jhmsClientKeytab = createJString(clientKeytab)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return false;
	}

	jni->CallVoidMethod(handle->objErrBuf, resetVal);
	result = jni->CallIntMethod(handle->objHmsClient, openConnectionWithKerberos,
							jhmsUris,
							jhmsServicePrincipal,
							jhmsClientPrincipal,
							jhmsClientKeytab,
							handle->objErrBuf,
							debug ? 1 : 0);

	jni->DeleteLocalRef(jhmsUris);
	jni->DeleteLocalRef(jhmsServicePrincipal);
	jni->DeleteLocalRef(jhmsClientPrincipal);
	jni->DeleteLocalRef(jhmsClientKeytab);

	if (result < 0)
	{
		HmsProcessErrorMessage(handle, NULL);
		return false;
	}

	return true;
}

void
HmsCloseConnection(HmsHandle *handle)
{
	jni->CallVoidMethod(handle->objHmsClient, closeConnection);
}

int
HmsTableExists(HmsHandle *handle, const char *dbName, const char *tableName, bool *exists)
{
	int result;
	jstring jvalBuf;
	char *val;
	jstring jdbName;
	jstring jtableName;

	if ((jdbName= createJString(dbName)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return -1;
	}

	if ((jtableName= createJString(tableName)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return -1;
	}

	jni->CallVoidMethod(handle->objErrBuf, resetVal);
	jni->CallVoidMethod(handle->objValBuf, resetVal);

	result = jni->CallIntMethod(handle->objHmsClient, tableExists,
							jdbName,
							jtableName,
							handle->objValBuf,
							handle->objErrBuf);

	jni->DeleteLocalRef(jdbName);
	jni->DeleteLocalRef(jtableName);

	if (result < 0)
	{
		HmsProcessErrorMessage(handle, NULL);
		return -1;
	}

	jvalBuf = (jstring) jni->CallObjectMethod(handle->objValBuf, getVal);
	val = (char *) jni->GetStringUTFChars(jvalBuf, NULL);
	if (strcmp(val, "1") == 0) {
		*exists = true;
	} else {
		*exists = false;
	}
	jni->ReleaseStringUTFChars(jvalBuf, val);

	return 0;
}

int
HmsGetTableMetaData(HmsHandle *handle,
					const char *dbName,
					const char *tableName)
{
	int result;
	jstring jerrBuf;
	jstring jdbName;
	jstring jtableName;

	if ((jdbName= createJString(dbName)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return -1;
	}

	if ((jtableName= createJString(tableName)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return -1;
	}

	handle->objMetaData = jni->NewObject(clsTable, consTable);
	if (handle->objMetaData == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return -1;
	}

	jni->CallVoidMethod(handle->objErrBuf, resetVal);
	result = jni->CallIntMethod(handle->objHmsClient, getTableMetaData,
							jdbName,
							jtableName,
							handle->objMetaData,
							handle->objErrBuf);

	jni->DeleteLocalRef(jdbName);
	jni->DeleteLocalRef(jtableName);

	if (result < 0)
	{
		HmsProcessErrorMessage(handle, NULL);
		return -1;
	}

	return 0;
}

void
HmsReleaseTableMetaData(HmsHandle *handle)
{
	if (handle->objMetaData)
		jni->DeleteLocalRef(handle->objMetaData);

	handle->objMetaData = NULL;
}

static char **
HmsPartTableGetList(HmsHandle *handle, jmethodID method, int index)
{
	jobjectArray elems;
	char **results = NULL;
	int i;
	int len;

	if (index == -1)
		elems = (jobjectArray) jni->CallObjectMethod(handle->objMetaData, method);
	else
		elems = (jobjectArray) jni->CallObjectMethod(handle->objMetaData, method, index);

	if (!elems)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return results;
	}

	len = jni->GetArrayLength(elems);
	results = (char **) palloc0_wrapper(sizeof(char *) * (len + 1));
	if (!results)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return results;
	}

	for (i = 0; i < len; i++)
	{
		jstring jelem = (jstring) jni->GetObjectArrayElement(elems, i);
		const char *elem = jni->GetStringUTFChars(jelem, NULL);

		results[i] = pstrdup_wrapper(elem);
		jni->ReleaseStringUTFChars(jelem, elem);
	}

	results[len] = NULL;

	return results;
}

char **
HmsPartTableGetKeys(HmsHandle *handle)
{
	return HmsPartTableGetList(handle, getPartKeys, -1);
}

char **
HmsPartTableGetKeyTypes(HmsHandle *handle)
{
	return HmsPartTableGetList(handle, getPartKeyTypes, -1);
}

char **
HmsTableGetLocations(HmsHandle *handle)
{
	return HmsPartTableGetList(handle, getLocations, -1);
}

char **
HmsPartTableGetKeyValues(HmsHandle *handle, int index)
{
	return HmsPartTableGetList(handle, getPartKeyValues, index);
}

char **
HmsPartTableGetPartValues(HmsHandle *handle, int index)
{
	return HmsPartTableGetList(handle, getPartValues, index);
}

char **
HmsPartTableGetFields(HmsHandle *handle)
{
	return HmsPartTableGetList(handle, getPartFields, -1);
}

char **
HmsGetTables(HmsHandle *handle, const char *dbName)
{
	jobjectArray elems;
	char **results = NULL;
	int i;
	int len;
	jstring jdbName;

	if ((jdbName = createJString(dbName)) == NULL)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return results;
	}

	jni->CallVoidMethod(handle->objErrBuf, resetVal);

	elems = (jobjectArray) jni->CallObjectMethod(handle->objHmsClient,
			getTables, jdbName, handle->objErrBuf);

	jni->DeleteLocalRef(jdbName);

	if (!elems)
	{
		HmsProcessErrorMessage(handle, NULL);
		return results;
	}

	len = jni->GetArrayLength(elems);
	results = (char **) palloc0_wrapper(sizeof(char *) * (len + 1));
	if (!results)
	{
		HmsProcessErrorMessage(handle, "out of memory");
		return results;
	}

	for (i = 0; i < len; i++)
	{
		jstring jelem = (jstring) jni->GetObjectArrayElement(elems, i);
		const char *elem = jni->GetStringUTFChars(jelem, NULL);

		results[i] = pstrdup_wrapper(elem);
		jni->ReleaseStringUTFChars(jelem, elem);
	}

	results[len] = NULL;

	return results;
}

bool
HmsTableIsPartitionTable(HmsHandle *handle)
{
	int result;

	result = jni->CallIntMethod(handle->objMetaData, isPartitionTable);

	return result == 1 ? true : false;
}

bool
HmsTableIsTransactionalTable(HmsHandle *handle)
{
	int result;

	result = jni->CallIntMethod(handle->objMetaData, isTransactionalTable);

	return result == 1 ? true : false;
}

static char *
getField_(HmsHandle *handle, jmethodID method)
{
	jstring jfield;
	const char *field;
	char *result;

	jfield = (jstring) jni->CallObjectMethod(handle->objMetaData, method);
	field = jni->GetStringUTFChars(jfield, NULL);

	result = pstrdup_wrapper(field);
	jni->ReleaseStringUTFChars(jfield, field);

	return result;
}

char *
HmsTableGetField(HmsHandle *handle)
{
	return getField_(handle, getField);
}

char *
HmsTableGetFormat(HmsHandle *handle)
{
	return getField_(handle, getFormat);
}

char *
HmsTableGetTableType(HmsHandle *handle)
{
	return getField_(handle, getTableType);
}

char *
HmsTableGetSerialLib(HmsHandle *handle)
{
	return getField_(handle, getSerialLib);
}

char *
HmsTableGetParameters(HmsHandle *handle)
{
	return getField_(handle, getParameters);
}

int
HmsPartTableGetNumber(HmsHandle *handle)
{
	int result;

	result = jni->CallIntMethod(handle->objMetaData, getPartitionNumber);

	return result;
}

char *
HmsTableGetLocation(HmsHandle *handle)
{
	return getField_(handle, getLocation);
}
