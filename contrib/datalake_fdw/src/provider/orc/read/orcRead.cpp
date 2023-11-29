#include "orcRead.h"

#include <list>
#include <cassert>
#include <sstream>

namespace Datalake {
namespace Internal {

void orcRead::createHandler(void *sstate)
{
	initParameter(sstate);
	options.transactionTable = scanstate->options->hiveOption->transactional;
	initializeColumnValue();
	createPolicy();
	deltaFile.readDeleteDeltaLists((void*)scanstate->options,
		readPolicy.deleteDeltaLists, setStreamWhetherCache(options));
	readNextGroup();
}

void orcRead::createPolicy()
{
	std::vector<ListContainer> lists;
	if (scanstate->options->hiveOption->hivePartitionKey != NULL)
	{
		List* fragment =  (List*)list_nth(scanstate->fragments, scanstate->options->hiveOption->curPartition);
		extraFragmentLists(lists, fragment);
		scanstate->options->hiveOption->curPartition += 1;
	}
	else
	{
		extraFragmentLists(lists, scanstate->fragments);
	}
	readPolicy.hiveTranscation = options.transactionTable;
	readPolicy.build(segId, segnum, BLOCK_POLICY_SIZE, lists);
	readPolicy.distBlock();
	blockSerial = readPolicy.start;
}

void orcRead::restart()
{
	tupleIndex = 0;
	stripeIndex = 0;
	last = false;
	fileReader.closeORCReader();
	readPolicy.reset();
	initializeColumnValue();
	createPolicy();
	deltaFile.reset();
	deltaFile.readDeleteDeltaLists((void*)scanstate->options,
		readPolicy.deleteDeltaLists, setStreamWhetherCache(options));
	readNextGroup();
}

int64_t orcRead::read(void *values, void *nulls)
{
nextPartition:

	if (readNextRow((Datum*)values, (bool*)nulls))
	{
		if (scanstate->options->hiveOption->hivePartitionKey != NULL)
		{
			AttrNumber numDefaults = this->nDefaults;
			int *defaultMap = this->defMap;
			ExprState **defaultExprs = this->defExprs;

			for (int i = 0; i < numDefaults; i++)
			{
				/* only eval const expr, so we don't need pg_try catch block here */
				Datum* value = (Datum*)values;
				bool* null = (bool*)nulls;
				value[defaultMap[i]] = ExecEvalConst(defaultExprs[i], NULL, &null[defaultMap[i]], NULL);
			}
		}
		return 1;
	}

	if (!isLastPartition(scanstate))
	{
		restart();
		goto nextPartition;
	}
	return 0;
}

fileState orcRead::getFileState()
{
	return fileReader.getState();
}

void orcRead::destroyHandler()
{
	tupleIndex = 0;
	releaseResources();
}

bool orcRead::readNextFile()
{
	if (blockSerial > readPolicy.end)
	{
		return false;
	}
	stripeIndex = 0;
	fileReader.readInterface.tempStripes.clear();
	auto it = readPolicy.block.find(blockSerial);
	if (it != readPolicy.block.end())
	{
		metaInfo info = it->second;
		if (info.exists)
		{
			if (info.fileLength <= info.blockSize)
			{
				return getStripeFromSmallFile(info);
			}
			else
			{
				return getStripeFromBigFile(info);
			}
		}
	}
	else
	{
		int64_t blockCount = readPolicy.block.size();
		elog(ERROR, "external table internal error. block index %d "
        "not found in orc block policy. block count %ld", blockSerial, blockCount);
	}
	return true;
}

bool orcRead::getStripeFromSmallFile(metaInfo info)
{
	if (!fileReader.createORCReader((void*)scanstate->options, info.fileName, info.fileLength, options))
	{
		/* file format not orc skip it */
		elog(LOG, "External table LOG, file %s format is not orc skip it.", info.fileName.c_str());
		return true;
	}

	for (uint64_t i = 0; i < fileReader.readInterface.reader->getNumberOfStripes(); i++)
	{
		fileReader.readInterface.tempStripes.push_back(fileReader.readInterface.reader->getStripe(i));
	}
	return true;
}

bool orcRead::getStripeFromBigFile(metaInfo info)
{
	if (curFileName != info.fileName)
	{
		fileReader.readInterface.stripes.clear();
		if (!fileReader.createORCReader((void*)scanstate->options, info.fileName, info.fileLength, options))
		{
			/* file format not orc skip it */
			elog(LOG, "External table LOG, file %s format is not orc skip it.", info.fileName.c_str());
			return true;
		}
		curFileName = info.fileName;
		for (uint64_t i = 0; i < fileReader.readInterface.reader->getNumberOfStripes(); i++)
		{
			ORC_UNIQUE_PTR<orc::StripeInformation> result = fileReader.readInterface.reader->getStripe(i);
			int64_t stripeOffset = result->getOffset();
			if (info.rangeOffset <= stripeOffset && stripeOffset <= info.rangeOffsetEnd)
			{
				fileReader.readInterface.tempStripes.push_back(fileReader.readInterface.reader->getStripe(i));
			}
			else
			{
				fileReader.readInterface.stripes.push_back(fileReader.readInterface.reader->getStripe(i));
			}
		}
	}
	else
	{
		int count = fileReader.readInterface.stripes.size();
		for (int i = 0; i < count; i++)
		{
			if (fileReader.readInterface.stripes[i] == NULL)
			{
				continue;
			}
			int64_t stripeOffset = fileReader.readInterface.stripes[i]->getOffset();
			if (info.rangeOffset <= stripeOffset && stripeOffset <= info.rangeOffsetEnd)
			{
				fileReader.readInterface.tempStripes.push_back(std::move(fileReader.readInterface.stripes[i]));
			}
		}
	}
	return true;
}

bool orcRead::getRow(Datum *values, bool *nulls)
{
	while (fileReader.readInterface.batch != NULL)
	{
		if (tupleIndex < (int64_t)fileReader.readInterface.batch->numElements)
		{
			if (options.transactionTable && fileReader.compareToDeleteMap(deltaFile, tupleIndex))
			{
				++tupleIndex;
                continue;
			}
			else
			{
				int nColumnsInFile = ncolumns - nPartitionKey;
				fileReader.read(tupdesc, values, nulls, tupleIndex, nColumnsInFile);
				tupleIndex++;
				return true;
			}
		}
		tupleIndex = 0;
		return false;
	}
	tupleIndex = 0;
	return false;
}

bool orcRead::getNextGroup()
{
	if (fileReader.getState() == FILE_CLOSE)
	{
		return false;
	}

	while(!fileReader.readInterface.rowReader || !fileReader.readInterface.rowReader->next(*fileReader.readInterface.batch))
	{
		fileReader.readInterface.rowReader.reset();
		if (stripeIndex < (int)fileReader.readInterface.tempStripes.size())
		{
			int nColumnsInFile = ncolumns - nPartitionKey;
			fileReader.readInterface.setRowReadOptions(attrs_used, nColumnsInFile);
			fileReader.readInterface.createRowReader(fileReader.readInterface.tempStripes[stripeIndex]->getOffset(),
				fileReader.readInterface.tempStripes[stripeIndex]->getLength());
			fileReader.readInterface.getTransactionTableType();
			stripeIndex++;
			continue;
		}
		else
		{
			return false;
		}
	}
	return true;
}



}
}