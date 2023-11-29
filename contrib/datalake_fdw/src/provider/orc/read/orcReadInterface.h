
#ifndef DATALAKE_ORCREADINTERFACE_H
#define DATALAKE_ORCREADINTERFACE_H

#include "orcInputStream.h"
#include <set>

#define ORC_BATCH_SIZE (1000)

namespace Datalake {
namespace Internal {
class orcReadInterface
{
public:
	orcReadInterface()
	{
		rowIndex = 0;
		stripesIndex = 0;
	}

	bool createORCReader(void* opt, std::string filename, int64_t length, bool enableCache, char *allocBuffer);

	void setRowReadOptions(std::set<int> attr_used, int column);

    void createRowReader(uint64_t stripeOffset, uint64_t stripteLength);

	bool getNextBatch();

	bool readRow(void *data);

    void getStripes();

    void deleteORCReader();

    void getTransactionTableType();

    orc::StructVectorBatch *getORCStructVectorBatch();

    void setTransactionTable(bool transaction);

    ORC_UNIQUE_PTR<orc::Reader> reader;
    ORC_UNIQUE_PTR<orc::RowReader> rowReader;
    ORC_UNIQUE_PTR<orc::ColumnVectorBatch> batch;
    orc::StructVectorBatch *orcVector;
    std::vector<ORC_UNIQUE_PTR<orc::StripeInformation>> stripes;
	std::vector<ORC_UNIQUE_PTR<orc::StripeInformation>> tempStripes;
    OssInputStream *stream;
    orc::ReaderOptions options;
    orc::RowReaderOptions rowOptions;
    const orc::Type *type;
    bool transactionTable;
    int stripesIndex;
    int rowIndex;

private:
	bool getNextStripes();

	void parseTranscationRow(void *data);
};

}
}
#endif