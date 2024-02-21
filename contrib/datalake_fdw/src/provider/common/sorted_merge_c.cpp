#include <string>
#include "sorted_merge_c.h"
#include "sorted_merge.h"

void *
createSortedMerge(char *filename, List *readers)
{
	std::string errorMessage;
	SortedMerge *sortedMerge = NULL;

	try
	{
		sortedMerge = new SortedMerge(filename, readers);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
        elog(ERROR, "failed to create sorted merge: %s", errorMessage.c_str());

	return sortedMerge;
}

bool
sortedMergeNext(void *sortedMerge, int64 *value)
{
	bool result;
	std::string errorMessage;
	SortedMerge *merge = (SortedMerge *) sortedMerge;

	try
	{
		result = merge->Next(value);
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
        elog(ERROR, "failed to call sortedMergeNext(): %s", errorMessage.c_str());

	return result;
}

void
sortedMergeClose(void *sortedMerge)
{
	std::string errorMessage;
	SortedMerge *merge = (SortedMerge *) sortedMerge;

	try
	{
		merge->Close();
		delete merge;
	}
	catch (std::exception &e)
	{
		errorMessage = e.what();
	}

	if (!errorMessage.empty())
        elog(ERROR, "failed to close sortedMerge: %s", errorMessage.c_str());
}
