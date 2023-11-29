#include "dataBufferArray.h"
#include <cstring>

extern "C" {
#include "src/datalake_def.h"
}
namespace Datalake {
namespace Internal {

void dataBufferArray::allocDataBufferArray(int columns)
{
    mColumns = columns;
    for (int i = 0; i < mColumns; i++)
    {
        dataBuffer *tmp = (dataBuffer *)palloc0(sizeof(dataBuffer));
        tmp->buffer = (char *)palloc0(BUFFER_LENGTH);
        tmp->length = BUFFER_LENGTH;
        mDataBuffer.push_back(tmp);
    }
}

void dataBufferArray::copyToDataBuffer(char *data, int size, int column)
{
    dataBuffer *ptr = mDataBuffer[column];
    if (size >= ptr->length)
    {
        // need resize buffer
        resizeDataBuffer(column, size);
    }
    ptr = mDataBuffer[column];
    memcpy(ptr->buffer, data, size);
    ptr->buffer[size] = '\0';
}

dataBuffer *dataBufferArray::getDataBuffer(int column)
{
    return mDataBuffer[column];
}

void dataBufferArray::resizeDataBuffer(int column, int length)
{
    dataBuffer *ptr = mDataBuffer[column];
    pfree(ptr->buffer);
    int resizeLength = length;
    ptr->buffer = (char *)palloc0(resizeLength);
    ptr->length = resizeLength;
}

void dataBufferArray::freeDataBuffer()
{
    for (int i = 0; i < mColumns; i++)
    {
        dataBuffer *ptr = mDataBuffer[i];
        pfree(ptr->buffer);
        pfree(ptr);
    }
}

}
}