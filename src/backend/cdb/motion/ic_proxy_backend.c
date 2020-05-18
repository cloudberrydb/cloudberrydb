/*-------------------------------------------------------------------------
 *
 * ic_proxy_backend.c
 *
 *    Interconnect Proxy Backend
 *
 * The functions in this file are all called by the backends.
 *
 * A backend and a client is the 2 end points of a domain socket, the backend
 * is in the QD/QE backend process, the client is in the proxy bgworker
 * process.
 *
 * TODO: at the moment the ic-tcp backend logic is reused by ic-proxy, in fact
 * the ic-proxy logic just lives inside ic_tcp.c, but in the future we may want
 * to build the ic-proxy logic independently, the potential benefits are:
 * - concurrent connection establishments: in current ic-tcp based
 *   implementation we establish the connections to the proxy one by one, it
 *   causes higher latency; in the new logic we could do them concurrently;
 * - send / receive as ICProxyPkt directly: ic-tcp sends with a 4-byte header,
 *   in the proxy bgworker we need to unpack them and repack them into
 *   ICProxyPkt packets; the receiver needs to do it reversely; if we send /
 *   receive as ICProxyPkt directly we could reduce the parsing and memory
 *   copying;
 *
 *
 * Copyright (c) 2020-Present Pivotal Software, Inc.
 *
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbgang.h"
#include "cdb/cdbvars.h"
#include "executor/execdesc.h"

#include "ic_proxy.h"
#include "ic_proxy_backend.h"
#include "ic_proxy_packet.h"
#include "ic_proxy_key.h"

#include <unistd.h>

/*
 * Connect to a proxy and finish the hand shaking.
 *
 * FIXME: this function is to make the debugging easier, however as it
 * establishes the connection, as well as the registeration, one by one, it has
 * bad performance.
 */
int
ic_proxy_backend_connect(ChunkTransportStateEntry *pEntry, MotionConn *conn)
{
	struct sockaddr_un addr;
	ICProxyKey	key;
	ICProxyPkt	pkt;
	int			fd;
	int			ret;
	char	   *data;
	int			size;

	ic_proxy_key_init(&key,								/* key */
					  gp_session_id,					/* sessionId */
					  gp_command_count,					/* commandId */
					  pEntry->sendSlice->sliceIndex,	/* sendSliceIndex */
					  pEntry->recvSlice->sliceIndex,	/* recvSliceIndex */
					  GpIdentity.segindex,				/* localContentId */
					  GpIdentity.dbid,					/* localDbid */
					  MyProcPid,						/* localPid */
					  conn->cdbProc->contentid,			/* remoteContentId */
					  conn->cdbProc->dbid,				/* remoteDbid */
					  conn->cdbProc->pid);				/* remotePid */

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	ic_proxy_build_server_sock_path(addr.sun_path, sizeof(addr.sun_path));

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd == PGINVALID_SOCKET)
		ic_proxy_log(ERROR, "ic-proxy-backend: fail to create a socket: %m");

	do
	{
		ret = connect(fd, (struct sockaddr *) &addr, sizeof(addr));

		if (ret == -1 && errno == EINTR)
			continue;

		if (ret == -1)
		{
			ic_proxy_log(LOG, "ic-proxy-backend: fail to connect to %s: %m",
						 addr.sun_path);
			pg_usleep(100000);
			continue;
		}
	} while (ret == -1);

	ic_proxy_message_init(&pkt, IC_PROXY_MESSAGE_HELLO, &key);

	/* send HELLO */
	data = (char *) &pkt;
	size = pkt.len;
	while (size > 0)
	{
		do
		{
			ret = send(fd, data, size, 0);
		} while (ret == -1 && errno == EINTR);
		if (ret == -1)
			ic_proxy_log(ERROR, "ic-proxy-backend: fail to send HELLO: %m");
		data += ret;
		size -= ret;
	}

	/* recv HELLO ACK */
	data = (char *) &pkt;
	size = sizeof(pkt);
	while (size > 0)
	{
		do
		{
			ret = recv(fd, data, size, 0);
		} while (ret == -1 && errno == EINTR);
		if (ret == -1)
			ic_proxy_log(ERROR, "ic-proxy-backend: fail to recv HELLO ACK: %m");
		else if (ret == 0)
		{
			ic_proxy_log(LOG, "ic-proxy-backend: the peer is not ready");

			shutdown(fd, SHUT_RDWR);
			close(fd);
			fd = -1;
			break;
		}
		data += ret;
		size -= ret;
	}

	return fd;
}
