#include <avro/Stream.hh>
#include <avro/Compiler.hh>
#include <sstream>

#include <boost/crc.hpp> // for boost::crc_32_type
#include <boost/iostreams/device/file.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/random/mersenne_twister.hpp>

#include <snappy.h>

#include "avroBlockReader.h"
#include "avroOssInputStream.h"

namespace {
const std::string AVRO_SCHEMA_KEY("avro.schema");
const std::string AVRO_CODEC_KEY("avro.codec");
const std::string AVRO_NULL_CODEC("null");
const std::string AVRO_DEFLATE_CODEC("deflate");
const std::string AVRO_SNAPPY_CODEC("snappy");

const size_t minSyncInterval = 32;
const size_t maxSyncInterval = 1u << 30;

boost::iostreams::zlib_params get_zlib_params()
{
    boost::iostreams::zlib_params ret;
    ret.method = boost::iostreams::zlib::deflated;
    ret.noheader = true;
    return ret;
}

typedef std::array<uint8_t, 4> Magic;
static const Magic magic = {{'O', 'b', 'j', '\x01'}};


} // namespace


static std::string toString(const std::vector<uint8_t> &v)
{
    std::string result;
    result.resize(v.size());
    copy(v.begin(), v.end(), result.begin());
    return result;
}

static avro::ValidSchema makeSchema(const std::vector<uint8_t> &v)
{
    std::istringstream iss(toString(v));
    avro::ValidSchema vs;
    avro::compileJsonSchema(iss, vs);
    return avro::ValidSchema(vs);
}

bool avroBlockReader::hasMoreBlock()
{
    const uint8_t *p = nullptr;
    size_t n = 0;
    if (!stream_->next(&p, &n))
    {
        eof_ = true;
        return false;
    }
    stream_->backup(n);
    return true;
}

avroBlockReader::avroBlockReader(std::unique_ptr<avro::SeekableInputStream> inputStream, const std::string &name):
                                filename_(name),
                                stream_(std::move(inputStream)),
                                decoder_(avro::binaryDecoder()),
                                dataDecoder_(avro::binaryDecoder())
{
    readHeader();
}

avroBlockReader::~avroBlockReader()
{
}


bool avroBlockReader::hasMoreData()
{
    if (eof_)
    {
        return false;
    }
    else if (objectCount_ != 0)
    {
        return true;
    }
    stream_->seek(blockEnd_);
    decoder_->init(*stream_);
    return false;
}

void avroBlockReader::readHeader()
{
    decoder_->init(*stream_);
    Magic m;
    avro::decode(*decoder_, m);
    if (magic != m)
    {
        throw avro::Exception("Invalid data file. Magic does not match: "
                        + filename_);
    }
    avro::decode(*decoder_, metadata_);
    Metadata::const_iterator it = metadata_.find(AVRO_SCHEMA_KEY);
    if (it == metadata_.end())
    {
        throw avro::Exception("No schema in metadata");
    }
    dataSchema_ = makeSchema(it->second);
    if (!readerSchema_.root())
    {
        readerSchema_ = dataSchema();
    }
    it = metadata_.find(AVRO_CODEC_KEY);
    if (it != metadata_.end() && toString(it->second) == AVRO_DEFLATE_CODEC)
    {
        codec_ = avro::DEFLATE_CODEC;
    }
    else if (it != metadata_.end() && toString(it->second) == AVRO_SNAPPY_CODEC)
    {
        codec_ = avro::SNAPPY_CODEC;
    }
    else
    {
        codec_ = avro::NULL_CODEC;
        if (it != metadata_.end() && toString(it->second) != AVRO_NULL_CODEC)
        {
            throw avro::Exception("Unknown codec in data file: " + toString(it->second));
        }
    }

    avro::decode(*decoder_, sync_);
    decoder_->init(*stream_);
    dataStart_ = stream_->byteCount();
}

bool avroBlockReader::readDataBlock(int64_t pos)
{
    seek(pos);
    return doReadDataBlock();
}

void avroBlockReader::seek(int64_t pos)
{
    stream_->seek(pos);
    eof_ = false;
    decoder_->init(*stream_);
}

bool avroBlockReader::checkSync()
{
    decoder_->init(*stream_);
    avro::DataFileSync s;
    avro::decode(*decoder_, s);
    if (s != sync_)
    {
        return false;
    }
    return true;
}

bool avroBlockReader::getDataBlockStartList(std::vector<int> &dataBlockList)
{
    seek(dataStart_);
    dataBlockList.clear();
    while (doReadDataBlock())
    {
        dataBlockList.push_back(blockStart_);
        seek(blockEnd_);
        if (!checkSync())
        {
            dataBlockList.clear();
            return false;
        }
    }
    objectCount_ = 0;
    return true;
}

bool avroBlockReader::doReadDataBlock()
{
    decoder_->init(*stream_);
    blockStart_ = stream_->byteCount();
    if (!hasMoreBlock())
    {
        return false;
    }

    avro::decode(*decoder_, objectCount_);
    int64_t byteCount_;
    avro::decode(*decoder_, byteCount_);
    decoder_->init(*stream_);
    blockEnd_ = stream_->byteCount() + byteCount_;
    std::unique_ptr<avro::InputStream> st = std::make_unique<BoundedInputStream>(*stream_, static_cast<size_t>(byteCount_));

    if (codec_ == avro::NULL_CODEC)
    {
        dataDecoder_->init(*st);
        dataStream_ = std::move(st);
    }
    else if (codec_ == avro::SNAPPY_CODEC)
    {
        boost::crc_32_type crc;
        uint32_t checksum = 0;
        compressed_.clear();
        uncompressed.clear();
        const uint8_t *data;
        size_t len;
        while (st->next(&data, &len)) {
            compressed_.insert(compressed_.end(), data, data + len);
        }
        len = compressed_.size();
        if (len < 4)
            throw avro::Exception("Cannot read compressed data, expected at least 4 bytes, got " + std::to_string(len));

        int b1 = compressed_[len - 4] & 0xFF;
        int b2 = compressed_[len - 3] & 0xFF;
        int b3 = compressed_[len - 2] & 0xFF;
        int b4 = compressed_[len - 1] & 0xFF;

        checksum = (b1 << 24) + (b2 << 16) + (b3 << 8) + (b4);
        if (!snappy::Uncompress(reinterpret_cast<const char *>(compressed_.data()),
                                len - 4, &uncompressed)) {
            throw avro::Exception(
                "Snappy Compression reported an error when decompressing");
        }
        crc.process_bytes(uncompressed.c_str(), uncompressed.size());
        uint32_t c = crc();
        if (checksum != c) {
            throw avro::Exception(
                boost::format("Checksum did not match for Snappy compression: Expected: %1%, computed: %2%") % checksum
                % c);
        }
        os_.reset(new boost::iostreams::filtering_istream());
        os_->push(
            boost::iostreams::basic_array_source<char>(uncompressed.c_str(),
                                                       uncompressed.size()));
        std::unique_ptr<avro::InputStream> in = avro::istreamInputStream(*os_);

        dataDecoder_->init(*in);
        dataStream_ = std::move(in);
    }
    else
    {
        compressed_.clear();
        const uint8_t *data;
        size_t len;
        while (st->next(&data, &len))
        {
            compressed_.insert(compressed_.end(), data, data + len);
        }
        os_.reset(new boost::iostreams::filtering_istream());
        os_->push(boost::iostreams::zlib_decompressor(get_zlib_params()));
        os_->push(boost::iostreams::basic_array_source<char>(
            compressed_.data(), compressed_.size()));

        std::unique_ptr<avro::InputStream> in = avro::nonSeekableIstreamInputStream(*os_);
        dataDecoder_->init(*in);
        dataStream_ = std::move(in);
    }
    return true;
}

