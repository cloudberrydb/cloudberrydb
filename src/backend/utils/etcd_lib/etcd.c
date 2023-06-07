/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include "postgres.h"

#ifndef USE_INTERNAL_FTS

#include <stdio.h>
#include <stdbool.h>
#include <string.h>
#include <curl/curl.h>
#include <jansson.h>
#include <pthread.h>
#include <stdint.h>

#include "utils/etcd.h"
#include "utils/etcdlib.h"
#include "common/base64.h"
#include "common/fe_memutils.h"

#define ETCD_JSON_NODE                  "node"
#define ETCD_JSON_PREVNODE              "prevNode"
#define ETCD_JSON_NODES                 "nodes"
#define ETCD_JSON_ACTION                "action"
#define ETCD_JSON_KEY                   "key"
#define ETCD_JSON_VALUE                 "value"
#define ETCD_JSON_DIR                   "dir"
#define ETCD_JSON_MODIFIEDINDEX         "modifiedIndex"
#define ETCD_JSON_INDEX                 "index"
#define ETCD_JSON_ERRORCODE             "errorCode"
#define ETCD_HEADER_INDEX               "X-Etcd-Index: "

#define ETCD_JSON_V3_HEADER             "header"
#define ETCD_JSON_V3_KVS                "kvs"
#define ETCD_JSON_V3_COUNT              "count"
#define ETCD_JSON_V3_DELETED            "deleted"
#define ETCD_JSON_V3_REVISION           "revision"
#define ETCD_JSON_V3_MOD_REVISION       "mod_revision"
#define ETCD_JSON_V3_LEASE_ID           "ID"
#define ETCD_JSON_V3_LEASE_TTL          "TTL"
#define ETCD_JSON_V3_MEMBER_ID           "member_id"
#define ETCD_JSON_V3_LEADER          "leader"

#define MAX_OVERHEAD_LENGTH           64
#define DEFAULT_CURL_TIMEOUT          10
#define DEFAULT_CURL_CONNECT_TIMEOUT  10

#define DEFAULT_ENABLE_ETCD_V3_API 1

#define ETCD_V32_URL_PREFIX "v3alpha"
#define ETCD_V33_URL_PREFIX "v3beta"
#define ETCD_V34_URL_PREFIX "v3"
#define DEFAULT_ETCD_URL_PREFIX ETCD_V33_URL_PREFIX

#define MAX_GLOBAL_HOSTNAME 128
#define MAX_ETCD_ENDPOINTS 32
#define MAX_ETCD_FAILOVER_RETRY 60
#define MAX_ETCD_FAILOVER_LOCK_TIMEOUT 10

#define NO_SUPPORT_IN_V2(ectd) \
    do { \
        if (!etcdlib_enable_v3(ectd)) return ETCDLIB_NOSUPPORT_IN_V2; \
    } \
    while(0);

#define NO_SUPPORT_IN_V3(ectd) \
    do { \
        if (etcdlib_enable_v3(ectd)) return ETCDLIB_NOSUPPORT_IN_V3; \
    } \
    while(0);

typedef enum {
    GET, PUT, DELETE, POST
} request_t;

static etcdlib_t *g_etcdlib = NULL;
static etcdlib_t *etcdlib_endpoints_handler[MAX_ETCD_ENDPOINTS] = {0};
static int etcdlib_endpoints_num = 0;
static char current_etcd_host[MAX_GLOBAL_HOSTNAME];
static pthread_mutex_t etcd_failover_lock = PTHREAD_MUTEX_INITIALIZER;
static bool etcd_failover_running = false;

static bool etcd_failover(etcdlib_t **etcdlib);
static etcdlib_t *etcdlib_create_internal(const char* server, int port, int flags);
static void etcdlib_destroy_internal(etcdlib_t *etcdlib);
static int etcdlib_get_internal(etcdlib_t *etcdlib, const char *key, char **value, int *modifiedIndex);
static int etcdlib_get_directory_internal(etcdlib_t *etcdlib, const char *directory, etcdlib_key_value_callback callback, void *arg, long long *modifiedIndex);
static int etcdlib_set_internal(etcdlib_t *etcdlib, const char *key, const char *value, long long ttl, bool prevExist);
static int etcdlib_refresh_internal(etcdlib_t *etcdlib, const char *key, int ttl);
static int etcdlib_watch_internal(etcdlib_t *etcdlib, const char *key, long long index, char **action, char **prevValue, char **value, char **rkey, long long *modifiedIndex);
static int etcdlib_del_internal(etcdlib_t *etcdlib, const char *key);
static int etcdlib_grant_lease_internal(etcdlib_t *etcdlib, long long *lease, int ttl);
static int etcdlib_renew_lease_internal(etcdlib_t *etcdlib, long long lease);
static int etcdlib_lock_internal(etcdlib_t *etcdlib, const char *name, long long lease, char **lock);
static int etcdlib_unlock_internal(etcdlib_t *etcdlib, const char *lock);

struct MemoryStruct {
    char *memory;
    char *header;
    size_t memorySize;
    size_t headerSize;
};

static
void memory_struct_init(struct MemoryStruct * memory) {
    memory->memory = palloc(1); /* will be grown as needed by the realloc above */
    memory->memorySize = 0; /* no data at this point */
    memory->header = NULL; /* will be grown as needed by the realloc above */
    memory->headerSize = 0; /* no data at this point */
}

static 
char *base64(const char *data,
                    int input_length,
                    int *output_length,
                    bool encode) {
    int b64_out_len;
    char * output;

    b64_out_len = encode ? pg_b64_enc_len(input_length) : pg_b64_dec_len(input_length);
    output = palloc0(b64_out_len + 1);
    
    b64_out_len = encode ? pg_b64_encode(data, input_length, output, b64_out_len):
        pg_b64_decode(data, input_length, output, b64_out_len);
    if (b64_out_len < 0) {
        if (output)
            pfree(output);
        if (output_length) {
            *output_length = -1;
        }
        return NULL;
    }

    output[b64_out_len] = '\0';

    if (output_length) {
        *output_length = b64_out_len;
    }

    return output;
}

static 
size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    struct MemoryStruct *mem = (struct MemoryStruct *) userp;

    mem->memory = repalloc(mem->memory, mem->memorySize + realsize + 1);
    if (mem->memory == NULL) {
        /* out of memory! */
        fprintf(stderr, "[ETCDLIB] Error: not enough memory (realloc returned NULL)\n");
        return 0;
    }

    memcpy(&(mem->memory[mem->memorySize]), contents, realsize);
    mem->memorySize += realsize;
    mem->memory[mem->memorySize] = 0;

    return realsize;
}

static size_t
WriteHeaderCallback(void *contents, size_t size, size_t nmemb, void *userp) {
    size_t realsize = size * nmemb;
    struct MemoryStruct *mem = (struct MemoryStruct *) userp;

    mem->header = repalloc(mem->header, mem->headerSize + realsize + 1);
    if (mem->header == NULL) {
        /* out of memory! */
        fprintf(stderr, "[ETCDLIB] Error: not enough header-memory (realloc returned NULL)\n");
        return 0;
    }

    memcpy(&(mem->header[mem->headerSize]), contents, realsize);
    mem->headerSize += realsize;
    mem->header[mem->headerSize] = 0;

    return realsize;
}

static int
performRequest(CURL **curl, char *url, request_t request, void *reqData, void *repData) {
    CURLcode res = 0;
    
    if (*curl == NULL) {
        *curl = curl_easy_init();
    } else {
        curl_easy_reset(*curl);
    }

    curl_easy_setopt(*curl, CURLOPT_NOSIGNAL, 1);
    curl_easy_setopt(*curl, CURLOPT_TIMEOUT, DEFAULT_CURL_TIMEOUT);
    curl_easy_setopt(*curl, CURLOPT_CONNECTTIMEOUT, DEFAULT_CURL_CONNECT_TIMEOUT);
    curl_easy_setopt(*curl, CURLOPT_FOLLOWLOCATION, 1L);
    /*curl_easy_setopt(*curl, CURLOPT_VERBOSE, 1L); */
    curl_easy_setopt(*curl, CURLOPT_URL, url);
    curl_easy_setopt(*curl, CURLOPT_WRITEFUNCTION, WriteMemoryCallback);
    curl_easy_setopt(*curl, CURLOPT_WRITEDATA, repData);
    if (((struct MemoryStruct *) repData)->header) {
        curl_easy_setopt(*curl, CURLOPT_HEADERDATA, repData);
        curl_easy_setopt(*curl, CURLOPT_HEADERFUNCTION, WriteHeaderCallback);
    }

    if (request == PUT) {
        curl_easy_setopt(*curl, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(*curl, CURLOPT_POST, 1L);
        curl_easy_setopt(*curl, CURLOPT_POSTFIELDS, reqData);
    } else if (request == DELETE) {
        curl_easy_setopt(*curl, CURLOPT_CUSTOMREQUEST, "DELETE");
    } else if (request == GET) {
        curl_easy_setopt(*curl, CURLOPT_CUSTOMREQUEST, "GET");
    } else if (request == POST) {
        curl_easy_setopt(*curl, CURLOPT_CUSTOMREQUEST, "POST");
        curl_easy_setopt(*curl, CURLOPT_POST, 1L);
        curl_easy_setopt(*curl, CURLOPT_POSTFIELDS, reqData);
    }

    res = curl_easy_perform(*curl);

    if (res != CURLE_OK && res != CURLE_OPERATION_TIMEDOUT) {
        const char *m = request == GET ? "GET" : request == PUT ? "PUT" : request == DELETE ? "DELETE" :
            request == POST ? "POST" :"?";
        fprintf(stderr, "[etcdlib] Curl error for %s @ %s: %s\n", url, m, curl_easy_strerror(res));

        curl_easy_cleanup(*curl);
        *curl = NULL;
    }

    return res;
}

/**
 * Static function declarations
 */
static int
performRequest(CURL **curl, char *url, request_t request, void *reqData, void *repData);
static size_t WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp);
/**
 * External function definition
 */

static bool
etcd_failover(etcdlib_t **etcdlib) {
    bool isEletedLeader = false;
    bool res = false;
    int retry_count = 0;
    struct timespec time_out;
    clock_gettime(CLOCK_REALTIME, &time_out);
    time_out.tv_sec += MAX_ETCD_FAILOVER_LOCK_TIMEOUT;

    /* etcd_failover_running flag ensures that there is only one failover request running at the same time.*/
    if (etcd_failover_running) {
        return true;
    }

    while (isEletedLeader == false && retry_count <= MAX_ETCD_FAILOVER_RETRY) {
        for (int i = 0; i < etcdlib_endpoints_num; i++) {
            if (strcmp(etcdlib_endpoints_handler[i]->host, current_etcd_host) == 0)
                continue;
            pthread_mutex_timedlock(&etcd_failover_lock, &time_out);
            etcd_failover_running = true;
            if (etcdlib_get_leader(etcdlib_endpoints_handler[i], &isEletedLeader) != ETCDLIB_RC_OK)
                continue;
            etcd_failover_running = false;
            pthread_mutex_unlock(&etcd_failover_lock);
            if (isEletedLeader) {
                printf("failoverHostLeaderFromETCD failover etcd connection to new leader: %s:%d\n", 
                      etcdlib_endpoints_handler[i]->host, etcdlib_endpoints_handler[i]->port);
                *etcdlib = etcdlib_endpoints_handler[i];
                res = true;
                break;
            }
        }
        sleep(1);
        retry_count ++;
    }
    return res;
}

/**
 * etcd_init
 */
int
etcd_init(etcdlib_endpoint_t *etcd_endpoints, int etcd_endpoints_num, int flags) {
    int status = 0;
    g_etcdlib = etcdlib_create(etcd_endpoints, etcd_endpoints_num, flags);
    if (!g_etcdlib)
        status = -1;
    return status;
}

static etcdlib_t *
etcdlib_create_internal(const char* server, int port, int flags) {
    
    curl_global_init(CURL_GLOBAL_ALL);

    etcdlib_t *lib = palloc(sizeof(*lib));
    lib->host = pstrdup(server);
    lib->port = port;
    lib->curl = NULL;
    lib->v3_api = DEFAULT_ENABLE_ETCD_V3_API;

    return lib;
}

etcdlib_t *
etcdlib_create(etcdlib_endpoint_t *etcd_endpoints, int etcd_endpoints_num, int flags) {
    etcdlib_endpoints_num = etcd_endpoints_num;
    etcdlib_endpoint_t *petcd_endpoints = etcd_endpoints;
    strncpy(current_etcd_host, petcd_endpoints->etcd_host, MAX_GLOBAL_HOSTNAME);
    for (int i = 0; i < etcd_endpoints_num; i++, petcd_endpoints++) {
        etcdlib_endpoints_handler[i] = etcdlib_create_internal(petcd_endpoints->etcd_host, petcd_endpoints->etcd_port, flags);
    }
    return etcdlib_endpoints_handler[0];
}

static void
etcdlib_destroy_internal(etcdlib_t *etcdlib) {
    if (etcdlib != NULL) {
        if (etcdlib->host) {
            pfree(etcdlib->host);
            etcdlib->host = NULL;
        }
    }
}

void
etcdlib_destroy(etcdlib_t *etcdlib) {
    for (int i = 0; i < etcdlib_endpoints_num; i++) {
       etcdlib_destroy_internal(etcdlib_endpoints_handler[i]);
    }
}

const char *
etcdlib_host(etcdlib_t *etcdlib) {
    return etcdlib->host;
}

int
etcdlib_port(etcdlib_t *etcdlib) {
    return etcdlib->port;
}

int
etcdlib_enable_v3(etcdlib_t *etcdlib) {
    return etcdlib->v3_api;
}

int
etcd_get(const char *key, char **value, int *modifiedIndex) {
    return etcdlib_get(g_etcdlib, key, value, modifiedIndex);
}

static int
etcdlib_get_request(etcdlib_t *etcdlib, const char *key, struct MemoryStruct *reply) {
    char *url = NULL;
    int res = -1;

    if (etcdlib_enable_v3(etcdlib)) {
        int key_encode_len;
        char *key_encode = NULL;
        char *request = NULL;

        key_encode = base64(key,strlen(key),&key_encode_len, true);
        if (!key_encode || key_encode_len == -1) {
            return res;
        }

        request = psprintf("{\"key\":\"%s\"}", key_encode);

        url = psprintf("http://%s:%d/%s/kv/range", etcdlib->host, etcdlib->port, DEFAULT_ETCD_URL_PREFIX);
        res = performRequest(&etcdlib->curl, url, POST, request, (void *) reply);

        if (request)
            pfree(request);
        if (key_encode)
            pfree(key_encode);
    } else {
        url = psprintf("http://%s:%d/v2/keys/%s", etcdlib->host, etcdlib->port, key);
        res = performRequest(&etcdlib->curl, url, GET, NULL, (void *) reply);
    }
    if (url)
        pfree(url);
    return res;
}

static int
etcdlib_get_decode_v3(etcdlib_t *etcdlib, struct MemoryStruct *reply, const char *key, char **value, int *modifiedIndex) {
    json_t *js_root = NULL;
    json_t *js_node = NULL;
    json_t *js_key = NULL;
    json_t *js_value = NULL;
    json_t *js_counts = NULL;
    json_t *js_modifiedIndex = NULL;
    json_error_t error;

    int retVal = ETCDLIB_RC_ERROR;
    int counts = 0;
    char * counts_str = NULL;
    char * modified_index_str = NULL;
    char * key_encode = NULL;
    char * value_str = NULL;

    key_encode = base64(key,strlen(key), NULL, true);
    if (!key_encode) {
        return ETCDLIB_RC_ERROR;
    }

    js_root = json_loads(reply->memory, 0, &error);
    if (!js_root) {
        pfree(key_encode);
        return ETCDLIB_JSON_DECODE_ERROR;
    }

    js_counts = json_object_get(js_root, ETCD_JSON_V3_COUNT);
    if (!js_counts) {
        retVal = ETCDLIB_JSON_STRUCT_ERROR;
        goto finish;
    }

    js_node = json_object_get(js_root, ETCD_JSON_V3_KVS);
    counts_str = pstrdup(json_string_value(js_counts));
    counts = atoi(counts_str);

    if (counts != 1 || js_node == NULL || json_array_size(js_node) != 1) {
        retVal = ETCDLIB_JSON_STRUCT_ERROR;
        goto finish;
    }

    js_node = json_array_get(js_node, 0);
    if (!js_node){
        retVal = ETCDLIB_JSON_STRUCT_ERROR;
        goto finish;
    }

    js_key = json_object_get(js_node, ETCD_JSON_KEY);
    js_value = json_object_get(js_node, ETCD_JSON_VALUE);
    
    if (!js_key || !js_value || \
        !json_is_string(js_key) || !json_is_string(js_value)  \
        || strlen(json_string_value(js_key)) != strlen(key_encode) || strcmp(json_string_value(js_key), key_encode) != 0) 
    {
        retVal = ETCDLIB_JSON_STRUCT_ERROR;
        goto finish;
    }

    if (modifiedIndex) {
        js_modifiedIndex = json_object_get(js_node, ETCD_JSON_V3_MOD_REVISION);
        if (!js_modifiedIndex) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto finish;
        }
        modified_index_str = pstrdup(json_string_value(js_modifiedIndex));
        *modifiedIndex = atoi(modified_index_str);
    }

    value_str = pstrdup(json_string_value(js_value));
    *value = base64(value_str, strlen(value_str), NULL, false);
    retVal = ETCDLIB_RC_OK;

finish:
    if (key_encode)
        pfree(key_encode);
    if (counts_str)
        pfree(counts_str);
    if (modified_index_str)
        pfree(modified_index_str);
    if (value_str)
        pfree(value_str);
    json_decref(js_root);

    return retVal;
}

static int
etcdlib_get_decode_v2(struct MemoryStruct *reply, char **value, int *modifiedIndex) {
    json_t *js_root = NULL;
    json_t *js_node = NULL;
    json_t *js_index = NULL;
    json_t *js_value = NULL;
    json_t *js_modifiedIndex = NULL;
    json_error_t error;

    int retVal = ETCDLIB_RC_ERROR;

    js_root = json_loads(reply->memory, 0, &error);
    if (!js_root) {
        return ETCDLIB_JSON_DECODE_ERROR;
    }

    js_node = json_object_get(js_root, ETCD_JSON_NODE);
    js_index = json_object_get(js_root, ETCD_JSON_INDEX);

    if (js_node != NULL) {
        js_value = json_object_get(js_node, ETCD_JSON_VALUE);
        js_modifiedIndex = json_object_get(js_node, ETCD_JSON_MODIFIEDINDEX);

        if (js_modifiedIndex != NULL && js_value != NULL) {
            if (modifiedIndex) {
                *modifiedIndex = json_integer_value(js_modifiedIndex);
            }
            *value = pstrdup(json_string_value(js_value));
            retVal = ETCDLIB_RC_OK;
        }
    } else if ((modifiedIndex != NULL) && (js_index != NULL)) {
        /* Error occurred, retrieve the index of ETCD from the error code */
        *modifiedIndex = json_integer_value(js_index);
        retVal = ETCDLIB_RC_OK;
    }

    json_decref(js_root);
    return retVal;
}

static int
etcdlib_get_decode(etcdlib_t *etcdlib, struct MemoryStruct *reply, const char *key, char **value, int *modifiedIndex) {
    
    return etcdlib_enable_v3(etcdlib) ? etcdlib_get_decode_v3(etcdlib, reply, key, value, modifiedIndex):
        etcdlib_get_decode_v2(reply, value, modifiedIndex);
}

int
etcdlib_get(etcdlib_t *etcdlib, const char *key, char **value, int *modifiedIndex) {
    int ret = etcdlib_get_internal(etcdlib, key, value, modifiedIndex);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_get_internal(etcdlib, key, value, modifiedIndex);
    }
    return ret;
}

static int
etcdlib_get_internal(etcdlib_t *etcdlib, const char *key, char **value, int *modifiedIndex) {
    int res = -1;
    struct MemoryStruct reply;

    memory_struct_init(&reply);

    int retVal = ETCDLIB_RC_ERROR;
    res = etcdlib_get_request(etcdlib, key, &reply);

    if (res == CURLE_OK) {
        retVal = etcdlib_get_decode(etcdlib, &reply, key, value, modifiedIndex);
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        /* process curl timeout case */
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        retVal = ETCDLIB_RC_ERROR;
        fprintf(stderr, "Error getting etcd value, curl error: '%s'\n", curl_easy_strerror(res));
    }

    if (reply.memory)
        pfree(reply.memory);

    if (retVal != ETCDLIB_RC_OK) {
        *value = NULL;
    }

    return retVal;
}

static int
etcd_get_recursive_values(json_t *js_root, etcdlib_key_value_callback callback, void *arg, json_int_t *mod_index) {
    json_t *js_nodes;
    if ((js_nodes = json_object_get(js_root, ETCD_JSON_NODES)) != NULL) {
        if (json_is_array(js_nodes)) {
            int len = json_array_size(js_nodes);
            for (int i = 0; i < len; i++) {
                json_t *js_object = json_array_get(js_nodes, i);
                json_t *js_mod_index = json_object_get(js_object, ETCD_JSON_MODIFIEDINDEX);

                if (js_mod_index != NULL) {
                    json_int_t index = json_integer_value(js_mod_index);
                    if (*mod_index < index) {
                        *mod_index = index;
                    }
                } else {
                    printf("[ETCDLIB] Error: No INDEX found for key!\n");
                }

                if (json_object_get(js_object, ETCD_JSON_NODES)) {
                    /* node contains nodes */
                    etcd_get_recursive_values(js_object, callback, arg, mod_index);
                } else {
                    /*else empty etcd directory, not an error.*/
                    json_t *js_key = json_object_get(js_object, ETCD_JSON_KEY);
                    json_t *js_value = json_object_get(js_object, ETCD_JSON_VALUE);

                    if (js_key && js_value) {
                        if (!json_object_get(js_object, ETCD_JSON_DIR)) {
                            callback(json_string_value(js_key), json_string_value(js_value), arg);
                        }
                    }
                }
            }
        } else {
            fprintf(stderr, "[ETCDLIB] Error: misformatted JSON: nodes element is not an array !!\n");
        }
    } else {
        fprintf(stderr, "[ETCDLIB] Error: nodes element not found!!\n");
    }

    return (*mod_index > 0 ? 0 : 1);
}

static long long
etcd_get_current_index(const char *headerData) {
    long long index = -1;
    char *indexStr = strstr(headerData, ETCD_HEADER_INDEX);
    indexStr += strlen(ETCD_HEADER_INDEX);

    if (sscanf(indexStr, "%lld\n", &index) == 1) {
    } else {
        index = -1;
    }
    return index;
}

int
etcd_get_directory(const char *directory, etcdlib_key_value_callback callback, void *arg, long long *modifiedIndex) {
    return etcdlib_get_directory(g_etcdlib, directory, callback, arg, modifiedIndex);
}

int
etcdlib_get_directory(etcdlib_t *etcdlib, const char *directory, etcdlib_key_value_callback callback, void *arg,
                          long long *modifiedIndex) {
    int ret = etcdlib_get_directory_internal(etcdlib, directory, callback, arg, modifiedIndex);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_get_directory_internal(etcdlib, directory, callback, arg, modifiedIndex);
    }
    return ret;
}

static int
etcdlib_get_directory_internal(etcdlib_t *etcdlib, const char *directory, etcdlib_key_value_callback callback, void *arg,
                          long long *modifiedIndex) {

    json_t *js_root = NULL;
    json_t *js_rootnode = NULL;
    json_t *js_index = NULL;

    json_error_t error;
    int res;
    struct MemoryStruct reply;

    NO_SUPPORT_IN_V3(etcdlib);
    memory_struct_init(&reply);

    int retVal = ETCDLIB_RC_OK;
    char *url = NULL;

    url = psprintf("http://%s:%d/v2/keys/%s?recursive=true", etcdlib->host, etcdlib->port, directory);
    res = performRequest(&etcdlib->curl, url, GET, NULL, (void *) &reply);

    if (res == CURLE_OK) {
        js_root = json_loads(reply.memory, 0, &error);
        if (js_root != NULL) {
            js_rootnode = json_object_get(js_root, ETCD_JSON_NODE);
            js_index = json_object_get(js_root, ETCD_JSON_INDEX);
        } else {
            retVal = ETCDLIB_RC_ERROR;
            fprintf(stderr, "[ETCDLIB] Error: %s in js_root not found\n", ETCD_JSON_NODE);
        }
        if (js_rootnode != NULL) {
            long long modIndex = 0;
            long long *ptrModIndex = NULL;
            if (modifiedIndex != NULL) {
                *modifiedIndex = 0;
                ptrModIndex = modifiedIndex;
            } else {
                ptrModIndex = &modIndex;
            }
            retVal = etcd_get_recursive_values(js_rootnode, callback, arg, (json_int_t *) ptrModIndex);
            long long indexFromHeader = etcd_get_current_index(reply.header);
            if (indexFromHeader > *ptrModIndex) {
                *ptrModIndex = indexFromHeader;
            }
        } else if ((modifiedIndex != NULL) && (js_index != NULL)) {
            // Error occurred, retrieve the index of ETCD from the error code
            *modifiedIndex = json_integer_value(js_index);;
        }
        if (js_root != NULL) {
            json_decref(js_root);
        }
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        retVal = ETCDLIB_RC_ERROR;
    }

    if (url)
        pfree(url);
    if (reply.memory)
        pfree(reply.memory);
    if (reply.header)
        pfree(reply.header);

    return retVal;
}

int
etcd_set(const char *key, const char *value, long long ttl, bool prevExist) {
    return etcdlib_set(g_etcdlib, key, value, ttl, prevExist);
}

static int
etcdlib_set_decode(etcdlib_t *etcdlib, const char * value, struct MemoryStruct *reply) {
    json_error_t error;
    json_t *js_root = NULL;
    json_t *js_node = NULL;
    json_t *js_value = NULL;
    int retVal = ETCDLIB_RC_ERROR;

    js_root = json_loads(reply->memory, 0, &error);

    if (!js_root) {
        return ETCDLIB_JSON_DECODE_ERROR;
    }
    
    if (etcdlib_enable_v3(etcdlib)) {
        js_node = json_object_get(js_root, ETCD_JSON_V3_HEADER);
        if (!js_node) {
            return ETCDLIB_JSON_STRUCT_ERROR;
        }
        js_value = json_object_get(js_node, ETCD_JSON_V3_REVISION);
        if (js_value != NULL){
            retVal = ETCDLIB_RC_OK;
        } 
    } else {
        js_node = json_object_get(js_root, ETCD_JSON_NODE);
        if (!js_node) {
            return ETCDLIB_JSON_STRUCT_ERROR;
        }

        js_value = json_object_get(js_node, ETCD_JSON_VALUE);
        if (js_value != NULL \
            && json_is_string(js_value)) {
            retVal = ETCDLIB_RC_OK;
        } else {
            return ETCDLIB_JSON_STRUCT_ERROR;
        }
    }

    json_decref(js_root);
    return retVal;
}

static int
etcdlib_set_request(etcdlib_t *etcdlib, const char *key, const char *value, long long ttl, bool prevExist, struct MemoryStruct * reply) {
    char *url = NULL;
    int res = -1;
    char *request = NULL;

    if (etcdlib_enable_v3(etcdlib)) {
        int key_encode_len, value_encode_len;
        char *key_encode = NULL, *value_encode = NULL; 

        key_encode = base64(key, strlen(key), &key_encode_len, true);
        value_encode = base64(value, strlen(value), &value_encode_len, true);

        if (!key_encode || !value_encode || key_encode_len == -1 || value_encode_len == -1) {
            goto done_v3;
        }

        request = psprintf("{\"key\":\"%s\", \"value\": \"%s\", \"lease\": \"%lld\"}", key_encode, value_encode, ttl);
        url = psprintf("http://%s:%d/%s/kv/put", etcdlib->host, etcdlib->port, DEFAULT_ETCD_URL_PREFIX);

        res = performRequest(&etcdlib->curl, url, POST, request, (void *) reply);

done_v3:
        if (request)
            pfree(request);
        if (key_encode)
            pfree(key_encode);
        if (value_encode)
            pfree(value_encode);
        
    } else {
        size_t req_len = strlen(value) + MAX_OVERHEAD_LENGTH;
        char *requestPtr;

        request = malloc(req_len);
        requestPtr = request;

        url = psprintf("http://%s:%d/v2/keys/%s", etcdlib->host, etcdlib->port, key);

        requestPtr += snprintf(requestPtr, req_len, "value=%s", value);
        if (ttl > 0) {
            requestPtr += snprintf(requestPtr, req_len - (requestPtr - request), ";ttl=%lld", ttl);
        }

        if (prevExist) {
            requestPtr += snprintf(requestPtr, req_len - (requestPtr - request), ";prevExist=true");
        }

        res = performRequest(&etcdlib->curl, url, PUT, request, (void *) reply);
        if (request)
            free(request);
    }

    if (url)
        pfree(url);
    return res;
}

int
etcdlib_set(etcdlib_t *etcdlib, const char *key, const char *value, long long ttl, bool prevExist) {
    int ret = etcdlib_set_internal(etcdlib, key, value, ttl, prevExist);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_set_internal(etcdlib, key, value, ttl, prevExist);
    }
    return ret;
}

static int
etcdlib_set_internal(etcdlib_t *etcdlib, const char *key, const char *value, long long ttl, bool prevExist) {
    int retVal = ETCDLIB_RC_ERROR;
    int res;
    struct MemoryStruct reply;

    if(!etcdlib_enable_v3(etcdlib)) {
        /* Skip leading '/', etcd v2 cannot handle this. */
        while (*key == '/') {
            key++;
        }
    }

    memory_struct_init(&reply);

    res = etcdlib_set_request(etcdlib, key, value, ttl, prevExist, &reply);

    if (res == CURLE_OK) {
        retVal = etcdlib_set_decode(etcdlib, value, &reply);
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        //timeout
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        retVal = ETCDLIB_RC_ERROR;
        fprintf(stderr, "Error getting etcd value, curl error: '%s'\n", curl_easy_strerror(res));
    }

    if (reply.memory)
        pfree(reply.memory);
    return retVal;
}

int
etcd_refresh(const char *key, int ttl) {
    return etcdlib_refresh(g_etcdlib, key, ttl);
}

int
etcdlib_refresh(etcdlib_t *etcdlib, const char *key, int ttl) {
    int ret = etcdlib_refresh_internal(etcdlib, key, ttl);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_refresh_internal(etcdlib, key, ttl);
    }
    return ret;
}

static int
etcdlib_refresh_internal(etcdlib_t *etcdlib, const char *key, int ttl) {
    int retVal = ETCDLIB_RC_ERROR;
    char *url = NULL;
    char *request;

    int res;
    struct MemoryStruct reply;

    NO_SUPPORT_IN_V3(etcdlib);

    /* Skip leading '/', etcd cannot handle this. */
    while (*key == '/') {
        key++;
    }

    memory_struct_init(&reply);

    url = psprintf("http://%s:%d/v2/keys/%s", etcdlib->host, etcdlib->port, key);
    request = psprintf("ttl=%d;prevExists=true;refresh=true", ttl);

    res = performRequest(&etcdlib->curl, url, PUT, request, (void *) &reply);
    if (res == CURLE_OK && reply.memory != NULL) {
        json_error_t error;
        json_t *root = json_loads(reply.memory, 0, &error);
        if (root != NULL) {
            json_t *errorCode = json_object_get(root, ETCD_JSON_ERRORCODE);
            if (errorCode == NULL) {
                //no curl error and no etcd errorcode reply -> OK
                retVal = ETCDLIB_RC_OK;
            } else {
                fprintf(stderr, "[ETCDLIB] errorcode %lli\n", json_integer_value(errorCode));
                retVal = ETCDLIB_RC_ERROR;
            }
            json_decref(root);
        } else {
            retVal = ETCDLIB_RC_ERROR;
            fprintf(stderr, "[ETCDLIB] Error: %s is not json\n", reply.memory);
        }
    }

    if (url)
        pfree(url);
    if (reply.memory)
        pfree(reply.memory);
    if (request)
        pfree(request);

    return retVal;
}

int
etcd_watch(const char *key, long long index, char **action, char **prevValue, char **value, char **rkey,
               long long *modifiedIndex) {
    return etcdlib_watch(g_etcdlib, key, index, action, prevValue, value, rkey, modifiedIndex);
}

int
etcdlib_watch(etcdlib_t *etcdlib, const char *key, long long index, char **action, char **prevValue, char **value, char **rkey,
               long long *modifiedIndex) {
    int ret = etcdlib_watch_internal(etcdlib, key, index, action, prevValue, value, rkey, modifiedIndex);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_watch_internal(etcdlib, key, index, action, prevValue, value, rkey, modifiedIndex);
    }
    return ret;
}

static int
etcdlib_watch_internal(etcdlib_t *etcdlib, const char *key, long long index, char **action, char **prevValue, char **value,
                  char **rkey, long long *modifiedIndex) {
    json_error_t error;
    json_t *js_root = NULL;
    json_t *js_node = NULL;
    json_t *js_prevNode = NULL;
    json_t *js_action = NULL;
    json_t *js_value = NULL;
    json_t *js_rkey = NULL;
    json_t *js_prevValue = NULL;
    json_t *js_modIndex = NULL;
    json_t *js_index = NULL;
    int retVal = ETCDLIB_RC_ERROR;
    char *url = NULL;
    int res;
    struct MemoryStruct reply;

    // don't use shared curl/mutex for watch, that will lock everything.
    CURL *curl = NULL;

    NO_SUPPORT_IN_V3(etcdlib);
    memory_struct_init(&reply);

    if (index != 0)
        url = psprintf("http://%s:%d/v2/keys/%s?wait=true&recursive=true&waitIndex=%lld", etcdlib->host, etcdlib->port,
                 key, index);
    else
        url = psprintf("http://%s:%d/v2/keys/%s?wait=true&recursive=true", etcdlib->host, etcdlib->port, key);

    res = performRequest(&curl, url, GET, NULL, (void *) &reply);

    curl_easy_cleanup(curl);

    if (res == CURLE_OK) {
        js_root = json_loads(reply.memory, 0, &error);

        if (js_root != NULL) {
            js_action = json_object_get(js_root, ETCD_JSON_ACTION);
            js_node = json_object_get(js_root, ETCD_JSON_NODE);
            js_prevNode = json_object_get(js_root, ETCD_JSON_PREVNODE);
            js_index = json_object_get(js_root, ETCD_JSON_INDEX);
            retVal = ETCDLIB_RC_OK;
        }
        if (js_prevNode != NULL) {
            js_prevValue = json_object_get(js_prevNode, ETCD_JSON_VALUE);
        }
        if (js_node != NULL) {
            js_rkey = json_object_get(js_node, ETCD_JSON_KEY);
            js_value = json_object_get(js_node, ETCD_JSON_VALUE);
            js_modIndex = json_object_get(js_node, ETCD_JSON_MODIFIEDINDEX);
        }
        if (js_prevNode != NULL) {
            js_prevValue = json_object_get(js_prevNode, ETCD_JSON_VALUE);
        }
        if ((prevValue != NULL) && (js_prevValue != NULL) && (json_is_string(js_prevValue))) {

            *prevValue = pstrdup(json_string_value(js_prevValue));
        }
        if (modifiedIndex != NULL) {
            if ((js_modIndex != NULL) && (json_is_integer(js_modIndex))) {
                *modifiedIndex = json_integer_value(js_modIndex);
            } else if ((js_index != NULL) && (json_is_integer(js_index))) {
                *modifiedIndex = json_integer_value(js_index);
            } else {
                *modifiedIndex = index;
            }
        }
        if ((rkey != NULL) && (js_rkey != NULL) && (json_is_string(js_rkey))) {
            *rkey = pstrdup(json_string_value(js_rkey));

        }
        if ((action != NULL) && (js_action != NULL) && (json_is_string(js_action))) {
            *action = pstrdup(json_string_value(js_action));
        }
        if ((value != NULL) && (js_value != NULL) && (json_is_string(js_value))) {
            *value = pstrdup(json_string_value(js_value));
        }
        if (js_root != NULL) {
            json_decref(js_root);
        }

    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        //ignore timeout
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        fprintf(stderr, "Got curl error: %s\n", curl_easy_strerror(res));
        retVal = ETCDLIB_RC_ERROR;
    }

    if (url)
        pfree(url);
    if (reply.memory)
        pfree(reply.memory);

    return retVal;
}

static int
etcdlib_del_request(etcdlib_t *etcdlib, const char *key, struct MemoryStruct *reply) {
    char *url = NULL;
    int res = -1;

    if (etcdlib_enable_v3(etcdlib)) {
        int key_encode_len;
        char *key_encode = NULL;
        char *request;

        key_encode = base64(key,strlen(key),&key_encode_len, true);
        if (!key_encode || key_encode_len == -1) {
            return res;
        }

        request = psprintf("{\"key\":\"%s\"}", key_encode);

        url = psprintf("http://%s:%d/%s/kv/deleterange", etcdlib->host, etcdlib->port, DEFAULT_ETCD_URL_PREFIX);
        res = performRequest(&etcdlib->curl, url, POST, request, (void *) reply);
        
        if (key_encode)
            pfree(key_encode);
        if (request)
            pfree(request);
    } else {
        url = psprintf("http://%s:%d/v2/keys/%s?recursive=true", etcdlib->host, etcdlib->port, key);
        res = performRequest(&etcdlib->curl, url, DELETE, NULL, (void *) &reply);
    }

    if (url)
        pfree(url);
    return res;
}

static int
etcdlib_del_decode(etcdlib_t *etcdlib, struct MemoryStruct *reply) {
    json_error_t error;
    json_t *js_root = NULL;
    json_t *js_node = NULL;
    int retVal = ETCDLIB_RC_ERROR;

    char * delete_str = NULL; 
    int delete_counts = 0; 

    js_root = json_loads(reply->memory, 0, &error);
    if (!js_root) {
        return ETCDLIB_JSON_DECODE_ERROR;
    }

    if (etcdlib_enable_v3(etcdlib)) {
        
        js_node = json_object_get(js_root, ETCD_JSON_V3_DELETED);
        if (!js_node) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }
        delete_str = pstrdup(json_string_value(js_node));
        delete_counts = atoi(delete_str);
        if (delete_counts != 1) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }

    } else {
        js_root = json_loads(reply->memory, 0, &error);
        if (!js_root) {
            retVal = ETCDLIB_JSON_DECODE_ERROR;
            goto ret;
        }
        js_node = json_object_get(js_root, ETCD_JSON_NODE);

        if (!js_node) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }
    }
    retVal = ETCDLIB_RC_OK;

ret:
    if (delete_str)
        pfree(delete_str);
    if (js_root)
        json_decref(js_root);
    return retVal;
}

int
etcd_del(const char *key) {
    return etcdlib_del(g_etcdlib, key);
}

int
etcdlib_del(etcdlib_t *etcdlib, const char *key) {
    int ret = etcdlib_del_internal(etcdlib, key);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_del_internal(etcdlib, key);
    }
    return ret;
}

static int
etcdlib_del_internal(etcdlib_t *etcdlib, const char *key) {
    int retVal = ETCDLIB_RC_ERROR;
    int res;
    struct MemoryStruct reply;

    memory_struct_init(&reply);

    res = etcdlib_del_request(etcdlib, key, &reply);

    if (res == CURLE_OK) {
        retVal = etcdlib_del_decode(etcdlib, &reply);
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        /* ignore timeout in case delete operation to not being triggered failover operation.*/
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        fprintf(stderr, "Got curl error: %s\n", curl_easy_strerror(res));
        retVal = ETCDLIB_RC_ERROR;
    }

    if (reply.memory)
        pfree(reply.memory);

    return retVal;
}

int
etcd_grant_lease(etcdlib_t *etcdlib, long long *lease, int ttl) {
    return etcdlib_grant_lease(g_etcdlib, lease, ttl);
}

int
etcdlib_grant_lease(etcdlib_t *etcdlib, long long *lease, int ttl) {
    int ret = etcdlib_grant_lease_internal(etcdlib, lease, ttl);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_grant_lease_internal(etcdlib, lease, ttl);
    }
    return ret;
}

static int
etcdlib_grant_lease_internal(etcdlib_t *etcdlib, long long *lease, int ttl) {
    struct MemoryStruct reply;
    int retVal = ETCDLIB_RC_ERROR;
    char *url = NULL;
    char *request = NULL;
    int res;

    json_error_t error;
    json_t *js_root = NULL;
    json_t *js_id = NULL, *js_ttl = NULL;
    char *ttl_str = NULL;
    int origin_lease = *lease;

    NO_SUPPORT_IN_V2(etcdlib);
    memory_struct_init(&reply);

    if (*lease == 0) {
        request = psprintf("{ \"TTL\": \"%d\"}", ttl);
    } else {
        request = psprintf("{\"lease\": \"%lld\", \"TTL\": \"%d\"}", *lease, ttl);
    }
    url = psprintf("http://%s:%d/%s/lease/grant", etcdlib->host, etcdlib->port, DEFAULT_ETCD_URL_PREFIX);

    res = performRequest(&etcdlib->curl, url, POST, request, (void *) &reply);

    if (res == CURLE_OK) {
        js_root = json_loads(reply.memory, 0, &error);
        if (!js_root) {
            retVal = ETCDLIB_JSON_DECODE_ERROR;
            goto ret;
        }

        js_id = json_object_get(js_root, ETCD_JSON_V3_LEASE_ID);
        js_ttl = json_object_get(js_root, ETCD_JSON_V3_LEASE_TTL);
        if (!js_id || !js_ttl) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }

        *lease = atoll(pstrdup(json_string_value(js_id)));
        if (*lease == 0 || ((origin_lease) != 0 && (*lease != origin_lease))) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }
        
        ttl_str = pstrdup(json_string_value(js_ttl));
        if (atoi(ttl_str) == -1) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }

        retVal = ETCDLIB_RC_OK;
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        fprintf(stderr, "Got curl error: %s\n", curl_easy_strerror(res));
        retVal = ETCDLIB_RC_ERROR;
    }

ret:
    if (ttl_str)
        pfree(ttl_str);
    if (url)
        pfree(url);
    if (reply.memory)
        pfree(reply.memory);
    if (request)
        pfree(request);
    if (js_root) {
        json_decref(js_root);
    }

    return retVal;
}

int
etcd_renew_lease(long long lease) {
    return etcdlib_renew_lease(g_etcdlib, lease);
}

int
etcdlib_renew_lease(etcdlib_t *etcdlib, long long lease) {
    int ret = etcdlib_renew_lease_internal(etcdlib, lease);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_renew_lease_internal(etcdlib, lease);
    }
    return ret;
}

static int
etcdlib_renew_lease_internal(etcdlib_t *etcdlib, long long lease) {
    struct MemoryStruct reply;
    int retVal = ETCDLIB_RC_ERROR;
    char * url = NULL;
    int res;

    json_error_t error;
    json_t *js_root = NULL;
    char *request = NULL;

    NO_SUPPORT_IN_V2(etcdlib);
    memory_struct_init(&reply);

    request = psprintf("{\"ID\":\"%lld\"}", lease);
    url = psprintf("http://%s:%d/%s/lease/keepalive", etcdlib->host, etcdlib->port, DEFAULT_ETCD_URL_PREFIX);
    res = performRequest(&etcdlib->curl, url, POST, request, (void *) &reply);

    if (res == CURLE_OK) {
        js_root = json_loads(reply.memory, 0, &error);

        if (!js_root) {
            retVal = ETCDLIB_JSON_DECODE_ERROR;
            goto ret;
        }
        
        retVal = ETCDLIB_RC_OK;
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        fprintf(stderr, "Got curl error: %s\n", curl_easy_strerror(res));
        retVal = ETCDLIB_RC_ERROR;
    }

ret:
    if (url)
        pfree(url);
    if (request)
        pfree(request);
    if (reply.memory)
        pfree(reply.memory);
    if (js_root) {
        json_decref(js_root);
    }
    return retVal;
}

int
etcd_lock(const char *name, long long lease, char **lock) {
    return etcdlib_lock(g_etcdlib, name, lease, lock);
}

int
etcdlib_lock(etcdlib_t *etcdlib, const char *name, long long lease, char **lock) {
    /*TODO: Since could not distinguish the following case with the same return error code, then not to implement failover function for lock API.
       1. Lock acquire failed caued by lock already occupied, it would return ETCDLIB_RC_TIMEOUT.
       2. Connection failed with etcd node, it would return ETCDLIB_RC_TIMEOUT.
    */
    int ret = etcdlib_lock_internal(etcdlib, name, lease, lock);
    if (ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_lock_internal(etcdlib, name, lease, lock);
    }
    return ret;
}

static int
etcdlib_lock_internal(etcdlib_t *etcdlib, const char *name, long long lease, char **lock) {
    struct MemoryStruct reply;
    int retVal = ETCDLIB_RC_ERROR;
    char * url = NULL;
    int res;
    int key_encode_len;
    char * key_encode = NULL;

    json_error_t error;
    json_t *js_root = NULL;
    json_t *js_node = NULL;
    char * lock_key_str = NULL;

    char *request = NULL;

    NO_SUPPORT_IN_V2(etcdlib);
    memory_struct_init(&reply);

    key_encode = base64(name, strlen(name), &key_encode_len, true);

    request = psprintf("{\"name\":\"%s\", \"lease\": \"%lld\"}", key_encode, lease);
    url = psprintf("http://%s:%d/%s/lock/lock", etcdlib->host, etcdlib->port, DEFAULT_ETCD_URL_PREFIX);
    res = performRequest(&etcdlib->curl, url, POST, request, (void *) &reply);

    if (res == CURLE_OK) {
        js_root = json_loads(reply.memory, 0, &error);
        if (!js_root) {
            retVal = ETCDLIB_JSON_DECODE_ERROR;
            goto ret;
        }

        js_node = json_object_get(js_root, ETCD_JSON_KEY);
        if (!js_node) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }

        lock_key_str = pstrdup(json_string_value(js_node));
        if (strlen(lock_key_str) == 0) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }
        if (lock) {
            *lock = lock_key_str;
        }

        retVal = ETCDLIB_RC_OK;
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        fprintf(stderr, "Got curl error: %s\n", curl_easy_strerror(res));
        retVal = ETCDLIB_RC_ERROR;
    }

ret:
    if (url)
        pfree(url);
    if (request)
        pfree(request);
    if (key_encode)
        pfree(key_encode);
    if (reply.memory)
        pfree(reply.memory);
    if (js_root) {
        json_decref(js_root);
    }
    return retVal;
}

int
etcd_unlock(const char *lock)  {
    return etcdlib_unlock(g_etcdlib, lock);
}

int
etcdlib_unlock(etcdlib_t *etcdlib, const char *lock) {
    int ret = etcdlib_unlock_internal(etcdlib, lock);
    if (ret == ETCDLIB_RC_TIMEOUT || ret == ETCDLIB_RC_ERROR) {
        if (etcdlib_endpoints_num > 1 && etcd_failover(&etcdlib))
            ret = etcdlib_unlock_internal(etcdlib, lock);
    }
    return ret;
}

static int
etcdlib_unlock_internal(etcdlib_t *etcdlib, const char *lock) {
    struct MemoryStruct reply;
    int retVal = ETCDLIB_RC_ERROR;
    char * url = NULL;
    int res;

    json_error_t error;
    json_t *js_root = NULL;
    json_t *js_node = NULL;
    char *request = NULL;

    NO_SUPPORT_IN_V2(etcdlib);
    memory_struct_init(&reply);

    request = psprintf("{\"key\":\"%s\"}", lock);
    url = psprintf("http://%s:%d/%s/lock/unlock", etcdlib->host, etcdlib->port, DEFAULT_ETCD_URL_PREFIX);
    res = performRequest(&etcdlib->curl, url, POST, request, (void *) &reply);

    if (res == CURLE_OK) {
        js_root = json_loads(reply.memory, 0, &error);

        if (!js_root) {
            retVal = ETCDLIB_JSON_DECODE_ERROR;
            goto ret;
        }

        js_node = json_object_get(js_root, ETCD_JSON_V3_HEADER);
        if (!js_node) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }

        js_node = json_object_get(js_node, ETCD_JSON_V3_REVISION);
        if (!js_node){
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        } 
        
        retVal = ETCDLIB_RC_OK;
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        fprintf(stderr, "Got curl error: %s\n", curl_easy_strerror(res));
        retVal = ETCDLIB_RC_ERROR;
    }

ret:
    if (url)
        pfree(url);
    if (request)
        pfree(request);
    if (reply.memory)
        pfree(reply.memory);
    if (js_root) {
        json_decref(js_root);
    }
    return retVal;
}

int
etcd_get_leader(bool *isLeader) {
    return etcdlib_get_leader(g_etcdlib, isLeader);
}

int
etcdlib_get_leader(etcdlib_t *etcdlib, bool *isLeader) {
    struct MemoryStruct reply;
    int retVal = ETCDLIB_RC_ERROR;
    char * url = NULL;
    long member = 0;
    long leader = 0;
    int res;

    json_error_t error;
    json_t *js_root = NULL;
    json_t *js_node = NULL;
    json_t *memberId = NULL, *leaderId = NULL;
    char *request = "{}";

    NO_SUPPORT_IN_V2(etcdlib);
    memory_struct_init(&reply);

    url = psprintf("http://%s:%d/%s/maintenance/status", etcdlib->host, etcdlib->port, DEFAULT_ETCD_URL_PREFIX);
    res = performRequest(&etcdlib->curl, url, POST, request, (void *) &reply);

   if (res == CURLE_OK) {
        js_root = json_loads(reply.memory, 0, &error);

        if (!js_root) {
            retVal = ETCDLIB_JSON_DECODE_ERROR;
            goto ret;
        }

        js_node = json_object_get(js_root, ETCD_JSON_V3_HEADER);
        if (!js_node) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }

        memberId= json_object_get(js_node, ETCD_JSON_V3_MEMBER_ID);
        leaderId = json_object_get(js_root, ETCD_JSON_V3_LEADER);
        if (!memberId || !leaderId) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }

        member = atoll(pstrdup(json_string_value(memberId)));
        leader = atoll(pstrdup(json_string_value(leaderId)));
        if (!member || !leader) {
            retVal = ETCDLIB_JSON_STRUCT_ERROR;
            goto ret;
        }

        if (member == leader) {
            *isLeader = true;
        }
        
        retVal = ETCDLIB_RC_OK;
    } else if (res == CURLE_OPERATION_TIMEDOUT) {
        retVal = ETCDLIB_RC_TIMEOUT;
    } else {
        fprintf(stderr, "Got curl error: %s\n", curl_easy_strerror(res));
        retVal = ETCDLIB_RC_ERROR;
    }

ret:
    if (url)
        pfree(url);
    if (reply.memory)
        pfree(reply.memory);
    if (js_root) {
        json_decref(js_root);
    }
    return retVal;
}

bool
test_etcd_failover(etcdlib_t **etcdlib) {
    return etcd_failover(etcdlib);
}

void
test_etcd_mock_handler(etcdlib_t **etcdlib, int etcd_endpoints) {
    int random_endpoints = rand()%etcd_endpoints;
    *etcdlib = etcdlib_endpoints_handler[random_endpoints];
}

#endif
