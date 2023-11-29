#ifndef DATALAKE_UTIL_H
#define DATALAKE_UTIL_H

#include "src/common/fileSystemWrapper.h"
#include <iostream>

std::string generateFileName(std::string prefix,
                             std::string gpossextName,
                             int segmentID,
                             int segmentFileNum,
                             std::string suffix);

int getFileMaxNumv2(ossFileStream file,
                  std::string prefix,
                  std::string gpossextName,
                  std::string suffix,
                  int segmentID);

char *strConvertLow(char *str);

#endif