#ifndef SORT_MERGE_H
#define SORT_MERGE_H

#include<inttypes.h>
#include <vector>
#include <queue>

struct List;
struct Reader;

using namespace std;

class SortedMerge {
public:
	SortedMerge(char *filename, List *readers);
	~SortedMerge();

	bool Next(int64_t *position);
	void Close();
private:
	bool filterNext(Reader *reader, int64_t *position);
	void addNext(Reader *reader);

private:
	char   *filename_;
	int     filenameSize_;
	Reader *reader_;
	priority_queue<pair<int64_t, Reader *>,
				   vector<pair<int64_t, Reader *>>,
				   greater<pair<int64_t, Reader *>>> heap_;
};

#endif // SORT_MERGE_H
