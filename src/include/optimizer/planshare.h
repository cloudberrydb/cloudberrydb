/* 
 * planshare.h
 * 		Prototypes and data structures for plan sharing
 * Copyright (c) 2007-2008, Greenplum inc
 */

#ifndef _PLANSHARE_H_
#define _PLANSHARE_H_

List *share_plan(PlannerInfo *root, Plan *common, int numpartners); 
Cost cost_share_plan(Plan *common, PlannerInfo *root, int numpartners);


#endif /* _PLANSHARE_H_ */

