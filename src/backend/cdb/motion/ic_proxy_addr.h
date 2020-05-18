/*-------------------------------------------------------------------------
 *
 * ic_proxy_addr.h
 *
 *
 * Copyright (c) 2020-Present Pivotal Software, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#ifndef IC_PROXY_ADDR_H
#define IC_PROXY_ADDR_H


typedef struct ICProxyAddr ICProxyAddr;


struct ICProxyAddr
{
	struct sockaddr_storage addr;

	int			dbid;
	int			content;
};


/*
 * List<ICProxyAddr *>
 */
extern List		   *ic_proxy_addrs;


extern void ic_proxy_reload_addresses(void);
extern int ic_proxy_get_my_port(void);
extern int ic_proxy_addr_get_port(const ICProxyAddr *addr);


#endif   /* IC_PROXY_ADDR_H */
