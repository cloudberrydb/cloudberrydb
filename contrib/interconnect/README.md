# Intercontect 

This subtree contains interconnect module && test && benchmark that different with other subtree inside {cbdb_src}/contrib. Other moudles are not part of the core CloudBerry system, but interconnect module split from `cdb module`, it **must be preload with CloudBerry**, otherwise CloudBerry system will not work properly.

**The interconnect module will be preloaded by default as a library.** When the compile option `--disable-preload-ic-module` is turned on, then the interconnect module will not be preloaded, then users need to add `interconnect` into guc `shared_preload_libraries`.


Benefits of Separating Interconnect Separately

- Independent & decoupling, easier to add new interconnect types
- It is more convenient to **test & debug & benchmark**.


# Intercontect interfaces and data structure

The main interface name is `MotionIPCLayer` defined in `ml_ipc.h`, there are already three implementations:
- tcp
- udpifc
- proxy 

The specific method can refer to the notes. Here is a diagram to describe the specific timing of the interface function being called.

```                                                                                                                                                           
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                                                                                                                                                                  
	                                      ┌─────────────┐         ┌─────────────────────┐                 ┌──────────────────────┐      ┌─────────────┐                               
	                                      │  cdb init   │────────▶│ InitMotionLayerIPC  │                 │  InitMotionLayerIPC  │◀─────│  cdb init   │                               
	                                      └─────────────┘         └─────────────────────┘                 └──────────────────────┘      └─────────────┘                               
	                                                                         │                                        │                                                               
	                                                                         │              ┌──────────┐              │                                                               
	                                                                         │              │ registe  │              │                                                               
	                                                                         │              │request(if│              │                                                               
	                                                                         ▼              │  have)   │              ▼                                                               
	                                      ┌─────────────┐         ┌────────────────────┐    │          │  ┌───────────────────────┐     ┌─────────────┐                               
	                                      │ motion exec │────────▶│ SetupInterconnect  │◀───┴──────────┴──│   SetupInterconnect   │◀────│ motion exec │                               
	                                      └─────────────┘         └────────────────────┘                  └───────────────────────┘     └─────────────┘                               
	                                                                         │                                        │                                                               
	                                                                         │                                        │                                                               
	                                                                         ▼                                        ▼                                                               
	                                                              ┌────────────────────┐                  ┌──────────────────────┐                                                    
	                                                              │SendTupleChunkToAMS │                  │RecvTupleChunkFromAny/│                                                    
	                                                              └────────────────────┘                  │  RecvTupleChunkFrom  │                                                    
	   ┌───────────────────────────┐                                         │         ┌─────────────────┐└──────────────────────┘                                                    
	   │ GetTransportDirectBuffer  │───┐            ┌───────────────┐        │         │ TupleChunkList  │            │       ┌───────────────┐                                       
	   └───────────────────────────┘   │            │ internal call │──────▶ │         └─────────────────┘            │◀──────│ internal call │                                       
	                 ▲                 │            └───────────────┘        ▼                  │                     ▼       └───────────────┘                                       
	                 │                 │                          ┌────────────────────┐        ▼         ┌──────────────────────┐      ┌──────────────────┐     ┌───────────────────┐
	                 │                 │            ┌─────────────│     SendChunk      │─────────────────▶│    RecvTupleChunk    │─────▶│ udp recv buffer  │────▶│ DirectPutRxBuffer │
	      ┌────────────────────┐       │            ▼             └────────────────────┘                  └──────────────────────┘      └──────────────────┘     └───────────────────┘
	      │   direct access    │       │    ┌──────────────┐                 │             ┌────────────┐             │                                                               
	      │   reduce memcpy    │       ├───▶│ buffer Pool  │                 │             │  stop or   │             │                                                               
	      └────────────────────┘       │    └──────────────┘                 ◀─────────────│ interrupt  │─────────────▶                                                               
	                 │                 │            ▲                        │             └────────────┘             │                                                               
	                 │                 │            │                        │                    │                   │                                                               
	                 ▼                 │            │             ┌────────────────────┐          │                   ▼                           ┌──────────────────┐                
	   ┌───────────────────────────┐   │            └──────▲──────│      SendEOS       │          │       ┌──────────────────────┐                │motion exec finish│                
	   │ PutTransportDirectBuffer  │───┘        ┌──────────┘      └────────────────────┘          │       │ TeardownInterconnect │◀───────────────│   or interrupt   │                
	   └───────────────────────────┘            │                            ▲                    │       └──────────────────────┘                └──────────────────┘                
	                                      ┌──────────┐                       │                    │                   │                                                               
	                                      │  flush   │                       │                    │                   │                                                               
	                                      └──────────┘                       │                    │                   ▼                                                               
	                                                              ┌────────────────────┐          │       ┌──────────────────────┐                ┌─────────────┐                     
	                                                              │  SendStopMessage   │          │       │CleanUpMotionLayerIPC │◀───────────────│ cdb cleanup │                     
	                                                              └────────────────────┘          │       └──────────────────────┘                └─────────────┘                     
	                                                                                              │                                                                                   
	                                                                                              │                                                                                   
	                          ┌──────────────────┐                                                │                                                                                   
	                          │motion exec finish│                ┌────────────────────┐          │                                                                                   
	                          │   or interrupt   │───────────────▶│TeardownInterconnect│◀─────────┘                                                                                   
	                          └──────────────────┘                └────────────────────┘                                                                                              
	                                                                         │                                                                                                        
	                                                                         ▼                                                                                                        
	                            ┌─────────────┐                   ┌─────────────────────┐                                                                                             
	                            │ cdb cleanup │──────────────────▶│CleanUpMotionLayerIPC│                                                                                             
	                            └─────────────┘                   └─────────────────────┘                                       
```


Notice:

- The ones starting with capital letters in the figure are methods in the interface.
- Notes starting with lowercase letters in the figure.

Interconnect contains three main data structures

- **ChunkTransportState**: Generated by `EState`, one-to-one correspondence with `EState` object.
   - Records most of the global information, such as remote connection information.
   - Contains a set of **ChunkTransportStateEntry**.
- **ChunkTransportStateEntry**: Generated by `SliceTable`, one-to-one correspondence with `motionId`.
   - Different interconnect implementations have different ** ChunkTransportStateEntry** objects, Obtain subclass objects through `CONTAINER_OF`.
	- In this structure, multiple links for managing a single motion node(Use `motionId` to identify). It is distinguished from incoming/outgoing
	- Contains a set of **MotionConn**
- **MotionConn**
	- Different interconnect implementations have different ** MotionConn** objects, Obtain subclass objects through `CONTAINER_OF`.
	- In this structure, a specific point-to-point connection is established.


# How to implements a interconnect type

Here I assume an optimization scenario: **in intercontect layer, support domain socket , used domain socket on local machine, and use udp implements when across machines**

Here are two pieces of pseudo-code to achieve this function

Solution1: call interfaces of `udpifc` in the `domain socket interface` which we will defined.

```
MotionIPCLayer domain_udp_ipc_layer = {
  .ic_type = INTERCONNECT_TYPE_DOMAIN_UDP, // new ic type
  .GetMaxTupleChunkSize = GetMaxTupleChunkSizeUDP, // udp header bigger than tcp, so use udp max size
  .GetListenPort = GetListenPortDomainUDP,  // need return a combined port. return type is `int32`, can hold both.


  /**
   * InitMotionIPCLayerDomainUDP() {
   *   // init domain socket ipc layer
   *   
   *   InitMotionIPCLayerUDP(); // also init udp ipc layer
   * }
   * 
   */ 
  .InitMotionLayerIPC = InitMotionIPCLayerDomainUDP, 

  /**
   * SetupInterconnectDomainUDP() {
   *   SetupInterconnectUDP(); // setup udp ipc layer before setup domain socket ipc layer
   *   // after SetupInterconnectUDP, `ChunkTransportState` have been inited
   *   // and some of `MotionConnUDP` have been init 
   *   
   *   SetupInterconnectDomainSocket(ChunkTransportState obj);
   *   // in SetupInterconnectDomainSocket should create some of object `MotionConnDomianUDP`
   *   // which contains `MotionConnUDP` and replace it in `ChunkTransportStateEntry->conns`
   * 
   *   // after this call, we can make sure that each `MotionConnDomianUDP` will use domain socket or udp?
   * }
   *
   */  
  .SetupInterconnect = SetupInterconnectDomainUDP,

  /**  
   * SendChunkDomainUDP() {
   *   // `MotionConnDomianUDP` use domain socket or udp send tuple
   * }
   */
  .SendTupleChunkToAMS = SendChunkDomainUDP,
  
  /**  
   * RecvTupleChunkDomainUDP() {
   *   // `MotionConnDomianUDP` use domain socket or udp recv tuple
   * }
   */  
  .RecvTupleChunk = RecvTupleChunkDomainUDP,


  ... other interfaces
}
```

Solution 2: Coupling the logic in `udpifc`, just like the current `proxy` implementation.

```
// still used udp interface 
// and add some struct inside `MotionConnUDP` + `ChunkTransportStateEntryUDP`
MotionIPCLayer udpifc_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_DOMAINUDP,
    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeUDP,
    .GetListenPort = GetListenPortUDP,
    .InitMotionLayerIPC = InitMotionIPCLayerUDP,
    .SetupInterconnect = SetupInterconnectUDP,
    .SendChunk = SendChunkUDPIFC,
    .RecvTupleChunk = RecvTupleChunkUDPIFC,
}

// do some hook like ic_proxy implements
InitMotionIPCLayerUDP/SetupInterconnect() {

// origin logic 
#ifdef ENABLE_DOMAIN_SOCKET
// init/setup domain socket
#endif

}

SendChunk/RecvTupleChunk() {
// inited MotionConnUDP will make sure used domain socket or udp
#ifdef ENABLE_DOMAIN_SOCKET
   if (conn->ShouldUseDomainSocket()) {
      // send with domain socket 
      return;
   }
#endif 

 // origin udp logic 
}

```




