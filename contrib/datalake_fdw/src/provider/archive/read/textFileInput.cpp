#include "textFileInput.h"

#include "textFileSnappyRead.h"
#include "textFileArchiveRead.h"
#include "textFileSimpleRead.h"
#include "textFileDeflateRead.h"

namespace Datalake {
namespace Internal {


std::shared_ptr<textFileInput> getTextFileInput(CompressType compress) {
    if (compress == SNAPPY) {
        return std::make_shared<textFileSnappyRead>();
    } else if (compress == DEFLATE) {
        return std::make_shared<textFileDeflateRead>();
    } else if (compress == UNCOMPRESS) {
		return std::make_shared<textFileSimpleRead>();
	} else {
        return std::make_shared<textFileArchiveRead>();
    }
}


}
}