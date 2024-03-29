#ifndef DATALAKE_LOGICALTYPE_H
#define DATALAKE_LOGICALTYPE_H

#include <parquet/api/reader.h>
#include "orc/OrcFile.hh"
#include "orc/Reader.hh"
#include "orc/Type.hh"
#include "orc/Vector.hh"
#include <parquet/api/reader.h>
#include <avro/Types.hh>
#include <avro/LogicalType.hh>
#include <avro/Node.hh>
#include <avro/Schema.hh>
#include <stdio.h>
#include <iostream>

namespace Datalake {
namespace Internal {


#define AVRO_NUMERIC_128_SIZE (38)
#define AVRO_NUMERIC_MAX_SIZE AVRO_NUMERIC_128_SIZE
#define AVRO_NUMERIC_DEFAULT_SIZE (AVRO_NUMERIC_MAX_SIZE + 1)

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

class AvroLogicalType : public logicalTypeBase
{
public:

	bool isType(const avro::NodePtr &node, avro::Type type, const avro::LogicalType &ltype);

	bool hasType(const avro::NodePtr &node, avro::Type type, const avro::LogicalType &ltype);

	bool checkDataTypeCompatible(const avro::NodePtr &node, avro::Type type, const avro::LogicalType &ltype);

	int8_t getNullBranchIdx(const avro::NodePtr &node);

	virtual std::string getTypeMappingSupported();
};

}
}
#endif //DATALAKE_LOGICALTYPE_H