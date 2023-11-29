#ifndef DATALAKE_LOGICALTYPE_H
#define DATALAKE_LOGICALTYPE_H

#include <parquet/api/reader.h>
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"


#include <stdio.h>
#include <iostream>

namespace Datalake {
namespace Internal {

class logicalTypeBase
{
public:
	virtual std::string getColTypeName(unsigned int type);

	virtual std::string getTypeMappingSupported();
};

class ORCLogicalType : public logicalTypeBase
{
public:
	bool checkDataTypeCompatible(unsigned int type, orc::TypeKind orcType);

	virtual std::string getTypeMappingSupported();

};

class ParquetLogicalType : public logicalTypeBase
{
public:
	bool checkDataTypeCompatible(unsigned int typeOid, ::parquet::Type::type type);

	virtual std::string getTypeMappingSupported();

};

}
}

#endif //DATALAKE_LOGICALTYPE_H