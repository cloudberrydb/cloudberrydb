#include "c.h"
#include "miscadmin.h"
#include "postgres.h"
#include "resowner.h"
#include "cdb/cdbdtxcontextinfo.h"
#include "cdb/cdbvars.h"
#include "executor/execdesc.h"
#include "storage/dsm.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/barrier.h"

#define UseSharedQueryPlan() (!QEDtxContextInfo.cursorContext && GpIdentity.segindex >= 0)

typedef struct SharedQueryPlanData
{
	int        session_id;
	dsm_handle handle;
	int        reference;
	int        commandCount;
} SharedQueryPlanEntry;

extern void InitSharedQueryPlanHash(void);
extern Size SharedQueryPlanHashSize(void);

void ReadSharedQueryPlan(char *serializedPlantree, int serializedPlantreelen, char *serializedQueryDispatchDesc, int serializedQueryDispatchDesclen);

void WriteSharedQueryPlan(const char *serializedPlantree, int serializedPlantreelen, const char *serializedQueryDispatchDesc, int serializedQueryDispatchDesclen, const SliceTable *sliceTable);

extern void AtAbort_SharedQueryPlan(void);