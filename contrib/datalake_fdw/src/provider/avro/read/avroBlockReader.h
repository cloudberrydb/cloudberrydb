#ifndef DATALAKE_AVRO_BLOCK_READER_H
#define DATALAKE_AVRO_BLOCK_READER_H

#include <avro/DataFile.hh>
#include <avro/Stream.hh>
#include <avro/Schema.hh>
#include "boost/utility.hpp"
#include "src/provider/provider.h"


class avroBlockReader : boost::noncopyable {
    const std::string filename_;
    const std::unique_ptr<avro::SeekableInputStream> stream_;
    const avro::DecoderPtr decoder_;
    int64_t objectCount_{};
    bool eof_;
    avro::Codec codec_;
    int64_t blockStart_{};
    int64_t blockEnd_{};
    int64_t dataStart_{};

    avro::ValidSchema readerSchema_;
    avro::ValidSchema dataSchema_;
    avro::ValidSchema resolveSchema_;
    avro::DecoderPtr dataDecoder_;
    std::unique_ptr<avro::InputStream> dataStream_;
    typedef std::map<std::string, std::vector<uint8_t>> Metadata;

    Metadata metadata_;
    avro::DataFileSync sync_{};

    // for compressed buffer
    std::unique_ptr<boost::iostreams::filtering_istream> os_;
    std::vector<char> compressed_;
    std::string uncompressed;
    void readHeader();
    bool doReadDataBlock();


public:

    bool readDataBlock(int64_t pos);

    /**
     * Returns the current decoder for this reader.
     */
    avro::Decoder &decoder() { return *dataDecoder_; }

    /**
     * Returns true if and only if there is more object in current block to read.
     */
    bool hasMoreData();

    /**
     * Returns true if and only if there is more block to read.
     */
    bool hasMoreBlock();

    /**
     * Decrements the number of objects yet to read.
     */
    void decr() { --objectCount_; }

    /**
     * Get the number of columns of the avro file
    */
    size_t getColumns() {return dataSchema_.root()->leaves();}

    /**
     * Constructs the reader for the given file.
     */
    explicit avroBlockReader(std::unique_ptr<avro::SeekableInputStream> inputStream, const std::string &name);

    ~avroBlockReader();

    /**
     * Returns the schema for this object.
     */
    const avro::ValidSchema &readerSchema() { return readerSchema_; }

    /**
     * Returns the schema stored with the data file.
     */
    const avro::ValidSchema &dataSchema() { return dataSchema_; }

    /**
     * Move to a specific, known synchronization point.
     */
    void seek(int64_t position);

    /**
     * Check the synchronization point at the blockEnd. 
     */
    bool checkSync();

    /**
     * Read a block info at the blockStart
     */

    bool getDataBlockStartList(std::vector<int> &blockStartList);


};

typedef std::unique_ptr<avroBlockReader> avroBlockReaderPtr;

#endif //DATALAKE_AVRO_BLOCK_READER_H
