#ifndef DATALAKE_DATABUFFERARRAY_H
#define DATALAKE_DATABUFFERARRAY_H

#include <stdio.h>
#include <vector>

#define BUFFER_LENGTH (32*1024)
namespace Datalake {
namespace Internal {

typedef struct dataBuff
{
    char *buffer;
    int length;
} dataBuffer;

class dataBufferArray
{
public:
	dataBufferArray() {
		mColumns = 0;
		mDataBuffer.clear();
	}

    void allocDataBufferArray(int columns);

    void copyToDataBuffer(char *data, int size, int column);

    void resizeDataBuffer(int column, int length);

    void freeDataBuffer();

    dataBuffer *getDataBuffer(int column);

private:
    std::vector<dataBuffer*> mDataBuffer;
    int mColumns;
};

}
}
#endif