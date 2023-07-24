/*-------------------------------------------------------------------------
 *
 * kmgr.h
 *
 * Portions Copyright (c) 2023, HashData Technology Limited.
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/crypto/kmgr.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef KMGR_H
#define KMGR_H

#include "common/kmgr_utils.h"

/* GUC parameters */
extern char *cluster_key_command;
extern bool	   tde_force_switch;

extern Size KmgrShmemSize(void);
extern void KmgrShmemInit(void);
extern void BootStrapKmgr(void);
extern void InitializeKmgr(void);
extern const CryptoKey *KmgrGetKey(int id);
extern bool CheckIsSM4Method(void);

#endif							/* KMGR_H */
