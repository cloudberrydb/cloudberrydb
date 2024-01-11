/*--------------------------------------------------------------------
 * main.c
 *
 * Copyright (c) 2016-Present Hashdata, Inc. 
 *
 * IDENTIFICATION
 *	  main.c
 *
 *--------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/printtup_vec.h"
#include "utils/guc.h"
#include "optimizer/planner.h"

#include "utils/guc_vec.h"
#include "hook/hook.h"
#include "utils/am_vec.h"
#include "optimizer/planner_vec.h"
#include "vecnodes/nodes.h"
#include "vecexecutor/executor.h"
#include "vecexecutor/nodeShareInputScan.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

void
_PG_init(void)
{
    DefineCustomIntVariable("vector.max_batch_size",
                            "max vectorization executor row count handle in one batch",
                            NULL,
                            &max_batch_size,
                            16384,
                            0, 163840,
                            PGC_USERSET,
							GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomBoolVariable("vector.enable_vectorization",
                             "Enables the planner's use of vectorized plans."
                             "Notice: gp_interconnect_queue_depth and gp_interconnect_snd_queue_depth"
                             "will be set to 64 when vectorization is enabled,"
                             "and restore to default value when disabled.",
                             NULL,
                             &enable_vectorization,
                             false,
                             PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
                             NULL, assign_enable_vectorization, NULL);
    DefineCustomBoolVariable("vector.force_vectorization",
                             "Force the planner's use of vectorized plans."
                             "If the plan produced by the current optimizer does not support vectorization, "
                             "it will switch to regenerate the plan using another optimizer. "
                             "If the plans produced by both optimizers cannot be vectorized, "
                             "the plan produced by the preset optimizer is used",
                             NULL,
                             &force_vectorization,
                             false,
                             PGC_USERSET,
							 GUC_GPDB_NEED_SYNC,
                             NULL, NULL, NULL);
    DefineCustomIntVariable("vector.min_concatenate_rows",
                            "Minimum number of rows to motion concatenate",
                            NULL,
                            &min_concatenate_rows,
                            max_batch_size,
                            0, 65536,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    DefineCustomIntVariable("vector.min_redistribute_handle_rows",
                            "Minimum number of rows to motion concatenate",
                            NULL,
                            &min_redistribute_handle_rows,
                            0,
                            0, 163840,
                            PGC_USERSET,
                            GUC_GPDB_NEED_SYNC,
                            NULL, NULL, NULL);
    planner_prev = planner_hook; 
    planner_hook = planner_hook_wrapper;

    vec_explain_prev = ExplainOneQuery_hook;
    ExplainOneQuery_hook = VecExplainOneQuery;

    vec_exec_start_prev = ExecutorStart_hook;
    ExecutorStart_hook = ExecutorStartWrapper;

    vec_exec_run_prev = ExecutorRun_hook;
    ExecutorRun_hook = ExecutorRunWrapper;

    vec_exec_end_prev = ExecutorEnd_hook;
    ExecutorEnd_hook = ExecutorEndWrapper;

    /* init vectorization am routine */
    InitAOCSVecHandler();

    RegisterXactCallback(dummy_schema_xact_cb, NULL);

}
