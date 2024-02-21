#include "sorted_merge.h"
extern "C" {
#include "postgres.h"
#include "src/dlproxy/datalake.h"
#include "nodes/pg_list.h"
#include "gopher/gopher.h"
#include "utils.h"
}

SortedMerge::SortedMerge(char *filename, List *readers)
	:filename_(filename), filenameSize_(strlen(filename)), reader_(NULL)
{
	ListCell *lc;

	if (list_length(readers) == 1)
	{
		reader_ = (Reader *)list_nth(readers, 0);
		return;
	}

	foreach(lc, readers)
	{
		Reader *reader = (Reader *) lfirst(lc);
		addNext(reader);
	}
}

SortedMerge::~SortedMerge() {}

bool
SortedMerge::Next(int64_t *position)
{
	if (reader_ != NULL)
		return filterNext(reader_, position);

	if (heap_.empty())
		return false;

	pair<uint64_t, Reader *> pair = heap_.top();
	heap_.pop();

	addNext(pair.second);
	*position = pair.first;
	return true;
}

void
SortedMerge::Close()
{
	if (reader_ != NULL)
	{
		reader_->Close(reader_);
		return;
	}

	while (!heap_.empty())
	{
		pair<uint64_t, Reader *> pair = heap_.top();
		heap_.pop();

		pair.second->Close(pair.second);
	}
}

bool
SortedMerge::filterNext(Reader *reader, int64_t *position)
{
	bool hasNext;
	int datafileSize;
	char *datafileName;
	bool shouldKeep;
	bool nulls[2] = {false, false};
	Datum values[2] = {0, 0};
	InternalRecord record = {values, nulls, 0};

	while (true)
	{
		hasNext = reader->Next(reader, &record);
		if (!hasNext)
			return false;

		datafileSize = VARSIZE_ANY_EXHDR(values[0]);
		datafileName = VARDATA_ANY(values[0]);

		shouldKeep = charSeqEquals(datafileName, datafileSize, filename_, filenameSize_);
		pfree(DatumGetPointer(values[0]));

		if (shouldKeep)
		{
			*position = DatumGetInt64(values[1]);
			return true;
		}
	}
}

void
SortedMerge::addNext(Reader *reader)
{
	int64_t position;

	bool hasNext = filterNext(reader, &position);
	if (!hasNext)
	{
		reader->Close(reader);
		return;
	}

	heap_.push(pair<int64_t, Reader *>(position, reader));
}
