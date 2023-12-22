extern "C" {
#include "src/datalake_option.h"
#include "src/datalake_def.h"
}

#include "fileSystemWrapper.h"
#include "fileSystem.h"
#include "util.h"
#include <exception>
#include <iostream>
#include <vector>

using Datalake::Internal::FileSystem;

struct ossInternalFileStream {
public:
	ossInternalFileStream(FileSystem *ctx) : context(ctx) {}

	~ossInternalFileStream()
	{
		if (context != NULL)
		{
			delete context;
			context = NULL;
		}
	}

	FileSystem &getContext()
	{
		return *context;
	}

	int type;

private:
	FileSystem *context;
};

#ifdef __cplusplus
extern "C" {
#endif

#define PARAMETER_ASSERT(para, eno) \
    if (!(para)) {  \
        errno = eno; \
        elog(ERROR, "External table Error, Parameter assert failed."); \
    }

ossFileStream createFileSystem(gopherConfig *conf)
{
	ossInternalFileStream *fileStream = NULL;

	try
	{
		FileSystem *file = new FileSystem();
		file->gopherCreateHandle(conf);
		fileStream = new ossInternalFileStream(file);
		fileStream->type = conf->ufs_type;
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to createFileSystem: %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to createFileSystem: internal error");
	}

	return fileStream;
}

int openFile(ossFileStream file, const char *path, int flag)
{
	PARAMETER_ASSERT(file != NULL && strlen(path) > 0, EINVAL);
	int ret = file->getContext().openFile(path, flag);
	return ret;
}

int writeFile(ossFileStream file, void *buff, int64_t size)
{
	PARAMETER_ASSERT(file != NULL, EINVAL);
	int ret = 0;
	try
	{
		ret = file->getContext().write(buff, size);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to write: %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to write: internal error");
	}
	return ret;
}

int readFile(ossFileStream file, void *buff, int64_t size)
{
	PARAMETER_ASSERT(file != NULL, EINVAL);
	int ret = 0;
	try
	{
		ret = file->getContext().read(buff, size);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to read: %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to read: internal error");
	}
	return ret;
}

int seekFile(ossFileStream file, int64_t postion)
{
	PARAMETER_ASSERT(file != NULL, EINVAL);
	int ret = 0;
	try
	{
		ret = file->getContext().seek(postion);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to seek: %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to seek: internal error");
	}
	return ret;
}

int closeFile(ossFileStream file)
{
	if (file == NULL)
	{
		return 0;
	}
	int ret = file->getContext().closeFile();
	return ret;
}

gopherFileInfo *listDir(ossFileStream file, const char *path, int *count, int recursive)
{
	PARAMETER_ASSERT(file != NULL, EINVAL);
    gopherFileInfo* result = NULL;
	try
	{
		result = file->getContext().listInfo(path, *count, recursive);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to list directory: %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to list directory: internal error");
	}

	return result;
}

void freeListDir(ossFileStream file, gopherFileInfo *list, int count)
{
	if (file == NULL)
		return;

	try
	{
		file->getContext().freeListInfo(list, count);
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to exec freeListDir(): %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to exec freeListDir(): internal error");
	}
}

gopherFileInfo* getFileInfo(ossFileStream file, const char* path)
{
    PARAMETER_ASSERT(file != NULL && strlen(path) > 0, EINVAL);

    // hdfs type
    std::string ufsPath;
    if (file->type == 9)
    {
        if (path[0] != '/')
        {
            std::string delimite = "/";
            ufsPath = delimite + path;
        }
        else
        {
            ufsPath = path;
        }
    }
    else
    {
        ufsPath = path;
    }

    gopherFileInfo* result = NULL;
    try
	{
        result = file->getContext().getFileInfo(ufsPath.c_str());
    }
	catch (std::exception &e)
	{
        elog(ERROR, "failed to exec getFileInfo(): %s", e.what());
    }
	catch (...)
	{
		elog(ERROR, "failed to exec getFileInfo(): internal error");
	}
    return result;
}

int getUfsId(ossFileStream file)
{
    if (file == NULL)
    {
        return 0;
    }
	int ret = 0;
	try
	{
		ret = file->getContext().getUfsId();
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to exec gopherDestroyHandle(): %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to exec gopherDestroyHandle(): internal error");
	}

	return ret;
}

int gopherDestroyHandle(ossFileStream file)
{
    if (file == NULL)
	{
        return 0;
    }
	int ret = 0;
	try
	{
		ret = file->getContext().gopherDestroyHandle();
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to exec gopherDestroyHandle(): %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to exec gopherDestroyHandle(): internal error");
	}

	return ret;
}

void destroyFileSystem(ossFileStream file)
{
	if (file == NULL)
		return;

	try
	{
		delete file;
		file = NULL;
	}
	catch (std::exception &e)
	{
		elog(ERROR, "failed to destroy ossFileStream: %s", e.what());
	}
	catch (...)
	{
		elog(ERROR, "failed to destory ossFileStream: internal error");
	}
}

void splitString(std::string inputStr, std::string delimiter, std::vector<std::string> &out)
{
	std::string str = inputStr + delimiter;
	size_t pos = str.find(delimiter);
	int step = delimiter.size();

	while(pos != str.npos)
	{
		std::string tmp = str.substr(0, pos);
		out.push_back(tmp);
		str = str.substr(pos + step, str.size());
		pos = str.find(delimiter);
	}
}

HdfsHAConfig* getHdfsHAConfig(gopherOptions *options)
{
	std::vector<std::string> namenodes;
	if (options->dfs_ha_namenodes)
	{
		splitString(options->dfs_ha_namenodes, ",", namenodes);
	}
	if (namenodes.size() == 0)
	{
		elog(WARNING, "Options set is_ha_supported is true, "
		"but cannot get dfs_ha_namenodes %s please check hdfs ha config.",
			options->dfs_ha_namenodes);
		options->hdfs_ha_configs_num = 0;
		return NULL;
	}

	options->hdfs_ha_configs_num = namenodes.size() + 3;

	HdfsHAConfig *hdfs_ha_configs = (HdfsHAConfig*)palloc0(sizeof(HdfsHAConfig) *
			options->hdfs_ha_configs_num);

	/* ha nameservices */
	hdfs_ha_configs[0].key = pstrdup("dfs.nameservices");
	hdfs_ha_configs[0].value = pstrdup(options->dfs_name_services);

	/* ha namenodes */
	char buffer[1024] = {0};
	sprintf(buffer, "dfs.ha.namenodes.%s", options->dfs_name_services);
	hdfs_ha_configs[1].key = pstrdup(buffer);
	hdfs_ha_configs[1].value = pstrdup(options->dfs_ha_namenodes);


	/* dfs client */
	sprintf(buffer, "dfs.client.failover.proxy.provider.%s", options->dfs_name_services);
	hdfs_ha_configs[2].key = pstrdup(buffer);
	hdfs_ha_configs[2].value = pstrdup(options->dfs_client_failover);


	/* ha namenode rpc addr */
	std::vector<std::string> rpc_addr;
	if (options->dfs_ha_namenode_rpc_addr)
	{
		splitString(options->dfs_ha_namenode_rpc_addr, ",", rpc_addr);
	}

	if (rpc_addr.size() != namenodes.size())
	{
		elog(ERROR, "Invalid Config dfs.namenode.rpc-address: %s and "
		"dfs.ha.namenodes %s , count not equal", options->dfs_ha_namenode_rpc_addr,
			options->dfs_ha_namenodes);
	}

	int num = 0;
	for (int i = 3; i < options->hdfs_ha_configs_num; i++)
	{
		sprintf(buffer, "dfs.namenode.rpc-address.%s.%s",
			options->dfs_name_services,
			namenodes[num].c_str());

		hdfs_ha_configs[i].key = pstrdup(buffer);
		hdfs_ha_configs[i].value = pstrdup(rpc_addr[num].c_str());
		num++;
	}

	return hdfs_ha_configs;
}

gopherConfig* createGopherConfig(void *opt)
{
	gopherOptions *options = (gopherOptions*)opt;
	gopherConfig* conf = (gopherConfig*)palloc0(sizeof(gopherConfig));
	conf->connect_path = pstrdup(options->connect_path);
	conf->connect_plasma_path = pstrdup(options->connect_plasma_path);
	conf->worker_path = pstrdup(options->worker_path);

	if (options->enableCache)
	{
		conf->cache_strategy = GOPHER_CACHE;
	}
	else
	{
		conf->cache_strategy = GOPHER_NOT_CACHE;
	}

	if (pg_strcasecmp(strConvertLow(options->gopherType), "hdfs") == 0)
	{
		conf->ufs_type = HDFS;

		conf->port = options->hdfs_namenode_port;

		if (options->hdfs_namenode_host)
		{
			std::vector<std::string> hostAndPort;
			splitString(options->hdfs_namenode_host, ":", hostAndPort);
			conf->name_node = pstrdup(hostAndPort[0].c_str());
			if (hostAndPort.size > 1)
			{
				conf->port = std::stoi(hostAndPort[1]);
			}
		}

		if (options->hdfs_auth_method)
		{
			conf->auth_method = pstrdup(options->hdfs_auth_method);
		}

		if (options->hadoop_rpc_protection)
		{
			conf->hadoop_rpc_protection = pstrdup(options->hadoop_rpc_protection);
		}

		if (strcmp(conf->auth_method, "kerberos") == 0)
		{
			if (options->krb_principal)
			{
				conf->krb_principal = pstrdup(options->krb_principal);
			}

			if (options->krb_principal_keytab)
			{
				conf->krb_server_key_file = pstrdup(options->krb_principal_keytab);
			}

			char krb5_ccname[1024] = {0};
			snprintf(krb5_ccname, 1024, "%s/krb5cc_%s", options->connect_path, conf->krb_principal);
			char* krb5Str = strstr(krb5_ccname, "krb5cc");
			int len = strlen(krb5Str);
			for (int i = 0; i < len; i++)
			{
				if (!isalnum(krb5Str[i]))
				{
					krb5Str[i] = '_';
				}
			}
			conf->krb5_ticket_cache_path = pstrdup(krb5_ccname);
		}

		conf->data_transfer_protocol = options->data_transfer_protocol;
		conf->is_ha_supported = options->is_ha_supported;

		if (options->is_ha_supported)
		{
			/* parser ha config */
			conf->hdfs_ha_configs = getHdfsHAConfig(options);
			conf->hdfs_ha_configs_num = options->hdfs_ha_configs_num;
		}
	}
	else
	{
		if (pg_strcasecmp(strConvertLow(options->gopherType), "qs") == 0)
		{
			conf->ufs_type = QINGSTOR;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "huawei") == 0)
		{
			conf->ufs_type = HUAWEI;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "ks3") == 0)
		{
			conf->ufs_type = KSYUN;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "cos") == 0)
		{
			conf->ufs_type = QCLOUD;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "ali") == 0)
		{
			conf->ufs_type = OSS;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "s3") == 0)
		{
			conf->ufs_type = S3A;
		}
		else if (pg_strcasecmp(strConvertLow(options->gopherType), "s3b") == 0)
		{
			conf->ufs_type = S3AV2;
		}
		else
		{
			ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("unkonw datalake platform \"%s\"", options->gopherType)));
		}

		if (options->bucket)
		{
			conf->bucket = pstrdup(options->bucket);
		}

		if (options->accessKey)
		{
			conf->access_key = pstrdup(options->accessKey);
		}

		if (options->secretKey)
		{
			conf->secret_key = pstrdup(options->secretKey);
		}

		if (options->host)
		{
			char endpoint[1024] = {0};
			if (options->port > 0)
			{
				char port[100] = {0};
				sprintf(port, "%d", options->port);
				std::string portStr = port;
				std::string ss = ":";
				portStr = ss + portStr;
				std::string hostStr = options->host;
				size_t pos = hostStr.find(portStr);
				if (pos > 0)
				{
					ereport(WARNING,
						errmsg("Please check the server options. Found port %d in the host %s. "
						"datalake support config host and port.",
							options->port, options->host));
				}
				snprintf(endpoint, 1024, "%s:%d", options->host, options->port);
			}
			else
			{
				snprintf(endpoint, 1024, "%s", options->host);
			}
			conf->endpoint = pstrdup(endpoint);
		}

		conf->useVirtualHost = options->useVirtualHost;
		conf->useHttps = options->useHttps;

		//TODO need add debug guc values gopher_oss_liboss2_log_level and gopher_oss_log_level

	}

	return conf;
}

void freeGopherConfig(gopherConfig* conf)
{
	if (conf)
	{
		if (conf->connect_path != NULL)
		{
			pfree(conf->connect_path);
			conf->connect_path = NULL;
		}

		if (conf->connect_plasma_path != NULL)
		{
			pfree(conf->connect_plasma_path);
			conf->connect_plasma_path = NULL;
		}

		if (conf->worker_path != NULL)
		{
			pfree(conf->worker_path);
			conf->worker_path = NULL;
		}

		if (conf->bucket != NULL)
		{
			pfree(conf->bucket);
			conf->bucket = NULL;
		}

		if (conf->access_key != NULL)
		{
			pfree(conf->access_key);
			conf->access_key = NULL;
		}

		if (conf->secret_key != NULL)
		{
			pfree(conf->secret_key);
			conf->secret_key = NULL;
		}

		if (conf->region != NULL)
		{
			pfree(conf->region);
			conf->region = NULL;
		}

		if (conf->endpoint != NULL)
		{
			pfree(conf->endpoint);
			conf->endpoint = NULL;
		}

		if (conf->local_path != NULL)
		{
			pfree(conf->local_path);
			conf->local_path = NULL;
		}

		if (conf->name_node != NULL)
		{
			pfree(conf->name_node);
			conf->name_node = NULL;
		}

		if (conf->uriPrefix != NULL)
		{
			pfree(conf->uriPrefix);
			conf->uriPrefix = NULL;
		}

		if (conf->auth_method != NULL)
		{
			pfree(conf->auth_method);
			conf->auth_method = NULL;
		}

		if (conf->krb_delegation_token != NULL)
		{
			pfree(conf->krb_delegation_token);
			conf->krb_delegation_token = NULL;
		}

		if (conf->krb5_ticket_cache_path != NULL)
		{
			pfree(conf->krb5_ticket_cache_path);
			conf->krb5_ticket_cache_path = NULL;
		}

		if (conf->krb_server_key_file != NULL)
		{
			pfree(conf->krb_server_key_file);
			conf->krb_server_key_file = NULL;
		}

		if (conf->krb_principal != NULL)
		{
			pfree(conf->krb_principal);
			conf->krb_principal = NULL;
		}

		if (conf->hadoop_rpc_protection != NULL)
		{
			pfree(conf->hadoop_rpc_protection);
			conf->hadoop_rpc_protection = NULL;
		}

		if ((conf->hdfs_ha_configs_num) > 0 && (conf->hdfs_ha_configs != NULL))
		{
			for (int i = 0; i < conf->hdfs_ha_configs_num; i++)
			{
				if (conf->hdfs_ha_configs[i].key)
				{
					pfree(conf->hdfs_ha_configs[i].key);
					conf->hdfs_ha_configs[i].key = NULL;
				}
				if (conf->hdfs_ha_configs[i].value)
				{
					pfree(conf->hdfs_ha_configs[i].value);
					conf->hdfs_ha_configs[i].value = NULL;
				}
			}
			pfree(conf->hdfs_ha_configs);
			conf->hdfs_ha_configs = NULL;
		}

		pfree(conf);
		conf = NULL;
	}
}

#ifdef __cplusplus
}
#endif
