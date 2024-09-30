/*-------------------------------------------------------------------------
 * ic_modules.c
 *	   Interconnect code shared between UDP, and TCP IPC Layers.
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 *
 *
 * IDENTIFICATION
 *	    contrib/interconnect/ic_modules.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "ic_modules.h"
#include "ic_internal.h"
#include "ic_common.h"
#include "tcp/ic_tcp.h"
#include "udp/ic_udpifc.h"

#ifdef ENABLE_IC_PROXY
#include "proxy/ic_proxy_server.h"
#endif

PG_MODULE_MAGIC;

MotionIPCLayer tcp_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_TCP,

    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeTCP,
    .GetListenPort = GetListenPortTCP,

    .InitMotionLayerIPC = InitMotionIPCLayerTCP,
    .CleanUpMotionLayerIPC = CleanUpMotionIPCLayerTCP,
    .WaitInterconnectQuit = WaitInterconnectQuitTCP,
    .SetupInterconnect = SetupInterconnectTCP,
    .TeardownInterconnect = TeardownInterconnectTCP,

    .SendTupleChunkToAMS = SendTupleChunkToAMS,
    .SendChunk = SendChunkTCP,
    .SendEOS = SendEOSTCP,
    .SendStopMessage = SendStopMessageTCP,

    .RecvTupleChunkFromAny = RecvTupleChunkFromAnyTCP,
    .RecvTupleChunkFrom = RecvTupleChunkFromTCP,
    .RecvTupleChunk = RecvTupleChunkTCP,

    .DirectPutRxBuffer = NULL,

    .DeregisterReadInterest = DeregisterReadInterestTCP,
    .GetActiveMotionConns = NULL,

    .GetTransportDirectBuffer = GetTransportDirectBuffer,
    .PutTransportDirectBuffer = PutTransportDirectBuffer,
#ifdef ENABLE_IC_PROXY
    .IcProxyServiceMain = ic_proxy_server_main,
#else 
    .IcProxyServiceMain = NULL,
#endif

    .GetMotionConnTupleRemapper = GetMotionConnTupleRemapper,
};

MotionIPCLayer proxy_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_PROXY,

    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeTCP,
    .GetListenPort = GetListenPortTCP,

    .InitMotionLayerIPC = InitMotionIPCLayerTCP,
    .CleanUpMotionLayerIPC = CleanUpMotionIPCLayerTCP,
    .WaitInterconnectQuit = WaitInterconnectQuitTCP,
    .SetupInterconnect = SetupInterconnectTCP,
    .TeardownInterconnect = TeardownInterconnectTCP,

    .SendTupleChunkToAMS = SendTupleChunkToAMS,
    .SendChunk = SendChunkTCP,
    .SendEOS = SendEOSTCP,
    .SendStopMessage = SendStopMessageTCP,

    .RecvTupleChunkFromAny = RecvTupleChunkFromAnyTCP,
    .RecvTupleChunkFrom = RecvTupleChunkFromTCP,
    .RecvTupleChunk = RecvTupleChunkTCP,

    .DirectPutRxBuffer = NULL,

    .DeregisterReadInterest = DeregisterReadInterestTCP,
    .GetActiveMotionConns = NULL,

    .GetTransportDirectBuffer = GetTransportDirectBuffer,
    .PutTransportDirectBuffer = PutTransportDirectBuffer,
#ifdef ENABLE_IC_PROXY
    .IcProxyServiceMain = ic_proxy_server_main,
#else 
    .IcProxyServiceMain = NULL,
#endif

    .GetMotionConnTupleRemapper = GetMotionConnTupleRemapper,
};


MotionIPCLayer udpifc_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_UDPIFC,

    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeUDP,
    .GetListenPort = GetListenPortUDP,

    .InitMotionLayerIPC = InitMotionIPCLayerUDP,
    .CleanUpMotionLayerIPC = CleanUpMotionLayerIPCUDP,
    .WaitInterconnectQuit = WaitInterconnectQuitUDPIFC,
    .SetupInterconnect = SetupInterconnectUDP,
    .TeardownInterconnect = TeardownInterconnectUDP,

    .SendTupleChunkToAMS = SendTupleChunkToAMS,
    .SendChunk = SendChunkUDPIFC,
    .SendEOS = SendEOSUDPIFC,
    .SendStopMessage = SendStopMessageUDPIFC,

    .RecvTupleChunkFromAny = RecvTupleChunkFromAnyUDPIFC,
    .RecvTupleChunkFrom = RecvTupleChunkFromUDPIFC,
    .RecvTupleChunk = RecvTupleChunkUDPIFC,

    .DirectPutRxBuffer = MlPutRxBufferIFC,

    .DeregisterReadInterest = DeregisterReadInterestUDP,
    .GetActiveMotionConns = GetActiveMotionConnsUDPIFC,

    .GetTransportDirectBuffer = GetTransportDirectBuffer,
    .PutTransportDirectBuffer = PutTransportDirectBuffer,
#ifdef ENABLE_IC_PROXY
    .IcProxyServiceMain = ic_proxy_server_main,
#else 
    .IcProxyServiceMain = NULL,
#endif

    .GetMotionConnTupleRemapper = GetMotionConnTupleRemapper,
};

MotionIPCLayer *ipc_layer_impls[] = {
	&tcp_ipc_layer,
	&udpifc_ipc_layer,
	&proxy_ipc_layer,
};

void
_PG_init(void)
{
	int elevel, impls_num;

	if (!process_shared_preload_libraries_in_progress) {
        ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not load interconnect outside process shared preload")));
    }

#ifdef USE_ASSERT_CHECKING
	elevel = WARNING;
#else
	elevel = LOG;
#endif

	impls_num = sizeof(ipc_layer_impls) / sizeof(MotionIPCLayer *);
	for (int i = 0; i < impls_num; ++i)
	{
		if (CurrentIPCLayerImplNum >= INTERCONNECT_TYPE_NUM)
		{
			elog(elevel,
				 "IPCLayerImpls[] is full, can not register more IPC layer implement.");
			break;
		}

		if (IsICTypeExist(ipc_layer_impls[i]->ic_type))
		{
			elog(elevel,
				 "ic_type: %d has been registered in IPCLayerImpls[].",
				 ipc_layer_impls[i]->ic_type);
			continue;
		}

		IPCLayerImpls[CurrentIPCLayerImplNum++] = ipc_layer_impls[i];
	}
}
