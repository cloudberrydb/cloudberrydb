#include "parquetRead.h"

namespace Datalake {
namespace Internal {

void parquetRead::createHandler(void *sstate)
{
	initParameter(sstate);
	initializeColumnValue();
	createPolicy();
	readNextGroup();
}

void parquetRead::createPolicy()
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
	blockPolicy.build(segId, segnum, BLOCK_POLICY_SIZE, lists);
	blockPolicy.distBlock();
	blockSerial = blockPolicy.start;
}

void parquetRead::restart()
{
	rowGroupNums.clear();
	tempRowGroupNums.clear();
	curRowGroupNum = 0;
	last = false;
	fileReader.closeParquetReader();
	blockPolicy.resetReadBlockPolicy();
	initializeColumnValue();
	createPolicy();
	readNextGroup();
}

bool parquetRead::getNextGroup()
{
	int size = tempRowGroupNums.size();
	for (; curRowGroupNum < size; curRowGroupNum++)
	{
		fileReader.setRowGroup(tempRowGroupNums[curRowGroupNum]);
		fileReader.createScanners();
		curRowGroupNum++;
		return true;
	}
	return false;
}

bool parquetRead::readNextFile()
{
	if (blockSerial > blockPolicy.end)
    {
        return false;
    }
    tempRowGroupNums.clear();
    curRowGroupNum = 0;
    auto it = blockPolicy.block.find(blockSerial);
    if (it != blockPolicy.block.end())
    {
        metaInfo info = it->second;
        if (info.exists)
        {
            if (info.fileLength <= info.blockSize)
            {
                return getRowGropFromSmallFile(info);
            }
            else
            {
                return getRowGropFromBigFile(info);
            }
        }
    }
    else
    {
        int64_t blockCount = blockPolicy.block.size();
        elog(ERROR, "external table internal error. block index %d "
        "not found in parquet block policy. block count %ld", blockSerial, blockCount);
    }

    return true;
}

bool parquetRead::getRowGropFromSmallFile(metaInfo info)
{
	fileReader.closeParquetReader();
    if (!fileReader.createParquetReader(fileStream, info.fileName, options))
    {
        /* this file format not parquet skip it. */
        elog(LOG, "External table LOG, file %s format is not parquet skip it.", info.fileName.c_str());
        return true;
    }

    for (int i = 0; i < fileReader.getRowGroupNums(); i++)
    {
        tempRowGroupNums.push_back(i);
    }
    return true;
}

bool parquetRead::getRowGropFromBigFile(metaInfo info)
{
    if (curFileName != info.fileName)
    {
        rowGroupNums.clear();
		fileReader.closeParquetReader();
        /* open next file */
        if (!fileReader.createParquetReader(fileStream, info.fileName, options))
        {
            /* this file format not parquet skip it. */
            elog(LOG, "External table LOG, file %s format is not parquet skip it.", info.fileName.c_str());
            return true;
        }
        curFileName = info.fileName;
        for (int i = 0; i < fileReader.getRowGroupNums(); i++)
        {
            int64_t offset = fileReader.rowGroupOffset(i);
            if (info.rangeOffset <= offset && offset <= info.rangeOffsetEnd)
            {
                tempRowGroupNums.push_back(i);
            }
            else
            {
                rowGroupNums.push_back(i);
            }
        }
    }
    else
    {
        /* is old file */
        int64_t count = rowGroupNums.size();
        for (int64_t i = 0; i < count; i++)
        {
            int64_t offset = fileReader.rowGroupOffset(rowGroupNums[i]);
            if (info.rangeOffset <= offset && offset <= info.rangeOffsetEnd)
            {
                tempRowGroupNums.push_back(rowGroupNums[i]);
            }
        }
    }
    return true;
}


int64_t parquetRead::read(void *values, void *nulls)
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

bool parquetRead::getRow(Datum *values, bool *nulls)
{
	return convertToDatum(values, nulls);
}

bool parquetRead::convertToDatum(Datum *values, bool *nulls)
{
	int nColumnsInFile = ncolumns - nPartitionKey;
    for (int i = 0; i < nColumnsInFile; i++)
    {
        if (options.nPartitionKey <= 0 && !options.includes_columns[i]) {
            nulls[i] = true;
            continue;
        }
        bool isNull = false;
        int state = 0;
        Datum value = fileReader.read(tupdesc->attrs[i].atttypid, i, isNull, state);
        if (state == -1)
        {
            /* read has not row, read next */
            return false;
        }
        values[i] = value;
        nulls[i] = isNull;
    }
    return true;
}

fileState parquetRead::getFileState()
{
	return fileReader.getState();
}

void parquetRead::destroyHandler()
{
    fileReader.closeParquetReader();
    destroyFileSystem(fileStream);
    fileStream = NULL;
	releaseResources();
}

}
}