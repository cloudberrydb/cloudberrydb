#include "rewrLogical.h"
#include <random>
#include <algorithm>

namespace Datalake {
namespace Internal {

void readLogical::releaseResources()
{
	if (defMap)
	{
		pfree(defMap);
	}
	if (defExprs)
	{
		pfree(defExprs);
	}
	if (includes_columns)
	{
		pfree(includes_columns);
	}
}

void readLogical::initParameter(void *sstate)
{
	scanstate = (dataLakeFdwScanState*)sstate;
	tupdesc = scanstate->rel->rd_att;
    ncolumns = tupdesc->natts;
	curFileName = "";
	segId = GpIdentity.segindex;
	segnum = getgpsegmentCount();

	last = false;
	options.buffer.allocDataBufferArray(ncolumns);
	options.enableCache = scanstate->options->gopher->enableCache;
	if (PROTOCOL_IS_HDFS(scanstate->options->gopher->gopherType))
	{
		options.ptype = PROTOTCOL_HDFS;
	}
	else
	{
		options.ptype = PROTOTCOL_OSS;
	}
	std::vector<ListContainer> lists;
	fileStream = createFileStream();

	getAttrsUsed(scanstate->retrieved_attrs, attrs_used);
	setOptionAttrsUsed(attrs_used);

	defMap = (int *)palloc0(ncolumns * sizeof(int));
	defExprs = (ExprState **)palloc0(ncolumns * sizeof(ExprState *));

	includes_columns = (bool*)palloc0(sizeof(bool) * ncolumns);
	for (int i = 0; i < ncolumns; i++)
	{
		includes_columns[i] = options.includes_columns[i];
	}

	return;
}

void readLogical::setOptionAttrsUsed(std::set<int> used)
{
	for (int i = 0; i < tupdesc->natts; i++)
	{
        /* Skip columns we don't intend to use in query */
        if (used.find(i) == used.end())
		{
			options.includes_columns.push_back(false);
			continue;
		}
		else
		{
			options.includes_columns.push_back(true);
		}
	}
}

void readLogical::initReadLogical()
{
	return;
}

bool readLogical::readNextFile()
{
	return true;
}

void readLogical::initializeColumnValue()
{
	MemoryContext oldContext = MemoryContextSwitchTo(scanstate->initcontext);
	List *constraints;
	List *attNums = scanstate->options->hiveOption->attNums;

	if (!scanstate->options->hiveOption->hivePartitionKey)
	{
		constraints = scanstate->options->hiveOption->constraints;
	}
	else
	{
		PartitionConstraint *pc = (PartitionConstraint*) list_nth(scanstate->options->hiveOption->hivePartitionConstraints,
			scanstate->options->hiveOption->curPartition);
		constraints = pc->constraints;
	}

	nPartitionKey = list_length(attNums);

	nDefaults = initializeDefaultMap(attNums,
						constraints,
						includes_columns,
						defMap,
						defExprs);
	nConstraint = nDefaults;

	options.nPartitionKey = nPartitionKey;
	MemoryContextSwitchTo(oldContext);
}

bool readLogical::readNextRow(Datum *values, bool *nulls)
{
	while((getFileState() == FILE_OPEN) && !last)
	{
		if (!getRow(values, nulls))
		{
			if (!readNextGroup())
			{
				/* read over */
				last = true;
				return false;
			}
		}
		else
		{
			setPartitionNulls(nulls);
			return true;
		}
	}
	return false;
}

fileState readLogical::getFileState()
{
	return FILE_UNDEF;
}

bool readLogical::getRow(Datum *values, bool *nulls)
{
	return true;
}

bool readLogical::readNextGroup()
{
	while(true)
	{
		if (getNextGroup())
		{
			return true;
		}

		if (!readNextFile())
		{
			last = true;
			return false;
		}
		blockSerial++;
	}
	return false;
}

bool readLogical::getNextGroup()
{
	return true;
}

void readLogical::closeFile()
{
	return;
}

void readLogical::createPolicy()
{
	return;
}

ossFileStream readLogical::createFileStream()
{
	gopherConfig *gopherConf = createGopherConfig((void*)(scanstate->options->gopher));
	ossFileStream fileStream = createFileSystem(gopherConf);
	freeGopherConfig(gopherConf);
	return fileStream;
}

void readLogical::extraFragmentLists(std::vector<ListContainer> &lists, List *fragmentLists)
{
	ListCell *cell;
	foreach(cell, fragmentLists)
	{
		ListContainer obj;
		List *fragment = (List*)lfirst(cell);
		obj.keyName = strVal(list_nth(fragment, 0));
		obj.size = atoi(strVal(list_nth(fragment, 1)));
		lists.push_back(obj);
	}
	sort(lists.begin(), lists.end(), cmp);
}

void readLogical::getAttrsUsed(List* retrieved_attrs, std::set<int> &attrs_used)
{
	ListCell   *frag_c = NULL;
	foreach(frag_c, retrieved_attrs)
	{
		int att = lfirst_int(frag_c) - 1;
		if (att < 0)
		{
			elog(ERROR, "get retrieved_attrs error value %d", att);
		}
		attrs_used.insert(att);
	}
}

int setStreamFlag(readOption opt)
{
	int flag = O_RDONLY|O_RDTHR;
	if (SUPPORT_ENABLE_CACHE(opt))
	{
		flag = O_RDONLY;
	}
	return flag;
}

bool setStreamWhetherCache(readOption opt)
{
	bool enableCache = false;
	if (SUPPORT_ENABLE_CACHE(opt))
	{
		enableCache = true;
	}
	return enableCache;
}

void readLogical::setPartitionNulls(bool* nulls)
{
	if (scanstate->options->hiveOption->hivePartitionKey != NULL)
	{
		int partitionColStart = ncolumns - nPartitionKey;
		for (int i = partitionColStart; i < ncolumns; ++i)
		{
			nulls[i] = !includes_columns[i];
		}
	}
	return;
}

}
}