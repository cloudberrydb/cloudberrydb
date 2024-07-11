#include "postgres.h"

#include <time.h>
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "utils/guc.h"
#include "src/datalake_def.h"
#include "random_segment.h"

static bool check_is_valid_random_num(int num_nodes, int random_num)
{
	if (random_num == 0 ||
		random_num < 0 ||
		random_num > num_nodes)
	{
		return false;
	}
	return true;
}

static List* no_need_random(int num_nodes, int random_num)
{
	int i;
	List* result = NIL;
	for (i = 0; i < num_nodes; i++)
	{
		// result = lappend_int(result, 1);
		result = lappend(result, makeInteger(1));
	}
	if (external_table_debug)
	{
		StringInfoData buff;
		initStringInfo(&buff);
		appendStringInfo(&buff, "external table no need "
			"limit segment. num_nodes %d random_num %d.", num_nodes, random_num);
		buff.data[strlen(buff.data) - 1] = '\0';
		elog(LOG, "%s", buff.data);
		pfree(buff.data);
	}
	return result;
}

static List* need_random(int num_nodes, int random_num)
{
	int i;
	int select_random_num = random_num;
	List* result = NIL;
	srand(time(NULL));
	// int selected[num_nodes];
	int *selected = (int *) palloc(num_nodes * sizeof(int));

	for (i = 0; i < num_nodes; i++)
	{
		selected[i] = 0;
	}

	for (i = 0; i < select_random_num; i++)
	{
		int index;
		do
		{
			index = rand() % num_nodes;
		} while(selected[index]);

		selected[index] = 1;
	}

	for (i = 0; i < num_nodes; i++)
	{
		// result = lappend_int(result, selected[i]);
		result = lappend(result, makeInteger(selected[i]));
	}
	pfree(selected);

	if (external_table_debug)
	{
		StringInfoData buff;
		initStringInfo(&buff);
		appendStringInfo(&buff, "external table select limit segment %d num_nodes %d.", select_random_num, num_nodes);
		int size = list_length(result);
		for (i = 0; i < size; i++)
		{
			if (list_nth_int(result, i))
			{
				appendStringInfo(&buff, " seg%d true ", i);
			}
			else
			{
				appendStringInfo(&buff, " seg%d false ", i);
			}
			buff.data[strlen(buff.data) - 1] = '\0';
			elog(LOG, "%s", buff.data);
			pfree(buff.data);
		}
	}
	return result;
}

List*
select_random_segments(int num_nodes, int random_num)
{
	if (!check_is_valid_random_num(num_nodes, random_num))
	{
		/* invalid random num */
		return no_need_random(num_nodes, random_num);
	}
	else
	{
		/* valid random num */
		return need_random(num_nodes, random_num);
	}

	/* should not reach here */
	return NIL;
}

void exec_segment(List* selectSegments, int cursegid, int cursegnum,
				  bool *exec, int *segindex, int *segnum) {
  int cursegindex = cursegid;
  int size = list_length(selectSegments);
  int selectNode = list_nth_int(selectSegments, cursegindex);
  if (selectNode == 1) {
	*exec = true;
  } else {
	*exec = false;
  }
  if (!*exec) {
    *segindex = 0;
  }

  int index = 0;
  int total = 0;
  for (int i = 0; i < size; i++) {
    if (i == cursegindex) {
      index = total;
    }
    if (list_nth_int(selectSegments, i)) {
      total++;
    }
  }
  *segindex = index;
  *segnum = total;
}
