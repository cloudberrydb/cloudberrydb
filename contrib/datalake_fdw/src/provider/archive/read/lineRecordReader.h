#ifndef DATALAKE_LINERECORDREADER_H
#define DATALAKE_LINERECORDREADER_H

#include "textFileInput.h"
// #include "src/Provider/common/distributeBlockBase.h"

namespace Datalake {
namespace Internal {

typedef enum lineRecordState {
    LINE_RECORD_INIT,
    LINE_RECORD_OK,
    LINE_RECORD_READ_BLOCK_OVER,
    LINE_RECORD_EXTRA_FETCH_LINE,
    LINE_CUSTOM_RECORD_INCOMPLETE_DELIMITER,
    LINE_RECORD_NOT_FOUND_DELIMTER,
    LINE_RECORD_FOUND_DELIMTER,
    LINE_RECORD_FETCH_FINAL_LINE
}lineRecordState;

class lineRecordReader {
public:
    lineRecordReader(int readLimit, char* recordDelimiter, datalakeMemoryBuffer *buffer) : start(0),
        end(0), pos(0), maxLineLength(0), maxBytesToConsume(0), readLimit(readLimit),
        curBytesConsumed(0), path(""), reader(NULL), state(LINE_RECORD_INIT),
        extra_fetch_line(false) {
        bufferStartPosn = 0;
        fileLength = 0;
        this->buffer = buffer;
        this->recordDelimiter = recordDelimiter;
        delPosn = 0;
        extra_fetch_line_num = -1;
        finalBlock = false;
    }

    void updateSplit(metaInfo info, bool finallyBlock);

    void updateNewReader(textFileInput* reader);

    lineRecordState getState() {
        return state;
    }

    void setState(lineRecordState states) {
        state = states;
    }

    int64_t getPos() {
        return pos;
    }

    bool next(int &length);

    int64_t readLine();

    int64_t fillBuffer();

    int64_t readDefaultLine();

    int64_t readCustomLine();

    int64_t getBufferStartPosn() {
        return bufferStartPosn;
    }

    void setSegId(int id) {
        segid = id;
    }

    bool customNext(int &length);

    bool defaultNext(int &length);

    void resetLineRecordReader();

private:
    char* reverse_strstr(const char* haystack, const char* needle);

    int64_t fetchPreviousBlockLastDelimiter();

    int64_t fetchLoopBeginBlockDelimiter(int64_t previousLastDelimiterPosn);

    int64_t processCrossBlockPosition();

private:
    int64_t start;
    int64_t end;
    int64_t pos;
    int maxLineLength;
    int64_t maxBytesToConsume;
    int readLimit;
    int64_t curBytesConsumed;
    std::string path;
    textFileInput* reader;
    lineRecordState state;
    char* recordDelimiter;
    bool extra_fetch_line;
    datalakeMemoryBuffer *buffer;
    int64_t bufferStartPosn;
    int64_t fileLength;
    int segid;
    int delPosn;
    int extra_fetch_line_num;
    bool finalBlock;
    bool loopBegin;
};

}
}



#endif