#include "logicalType.h"


extern "C" {
#include "src/datalake_def.h"
}

namespace Datalake {
namespace Internal {

std::string logicalTypeBase::getColTypeName(unsigned int type)
{
	switch (type)
    {
		case BOOLOID:
			return "boolean<bool>";
		case INT8OID:
			return "bigint<int8>";
		case INT4OID:
			return "integer<int, int4>";
		case INT2OID:
			return "smallint<int2>";
		case FLOAT4OID:
			return "real<float4>";
		case FLOAT8OID:
			return "double<float8>";
		case DATEOID:
			return "date";
		case INTERVALOID:
			return "double<float8>";
		case TIMEOID:
			return "interval";
		case TIMESTAMPOID:
			return "timestamp";
		case NUMERICOID:
			return "numeric<decimal>";
		case CHAROID:
			return "character<char>";
		case BYTEAOID:
			return "bytea";
		case TEXTOID:
			return "text";
		case BPCHAROID:
			return "character(n)<char(n)>";
		case VARCHAROID:
			return "character varying(n)<varchar(n)>";
		default:
			return "OSS protocol not supported data type.";
    }
}

std::string logicalTypeBase::getTypeMappingSupported()
{
	return "";
}

bool ORCLogicalType::checkDataTypeCompatible(unsigned int type,  orc::TypeKind orcType)
{
	switch (type)
    {
		case BOOLOID:
		case INT8OID:
		case INT4OID:
		case INT2OID:
		{
			if ((orcType != orc::TypeKind::BOOLEAN) && (orcType != orc::TypeKind::BYTE) && (orcType != orc::TypeKind::SHORT) && (orcType != orc::TypeKind::INT) && (orcType != orc::TypeKind::LONG) && (orcType != orc::TypeKind::DATE))
			{
				return false;
			}
			return true;
		}
		case FLOAT4OID:
		case FLOAT8OID:
		{
			if ((orcType != orc::TypeKind::FLOAT) && (orcType != orc::TypeKind::DOUBLE))
			{
				return false;
			}
			return true;
		}
		case DATEOID:
		{
			if ((orcType != orc::TypeKind::DATE))
			{
				return false;
			}
			return true;
		}
		case INTERVALOID:
		case TIMEOID:
		{
			if ((orcType != orc::TypeKind::STRING) && (orcType != orc::TypeKind::VARCHAR))
			{
				return false;
			}
			return true;
		}
		case TIMESTAMPOID:
		{
			if ((orcType != orc::TypeKind::TIMESTAMP))
			{
				return false;
			}
			return true;
		}
		case NUMERICOID:
		{
			if ((orcType != orc::TypeKind::DECIMAL))
			{
				return false;
			}
			return true;
		}
		case CHAROID:
		case BYTEAOID:
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
		{
			if ((orcType != orc::TypeKind::BINARY) && (orcType != orc::TypeKind::CHAR) && (orcType != orc::TypeKind::STRING) && (orcType != orc::TypeKind::VARCHAR))
			{
				return false;
			}
			return true;
		}
		default:
			return false;
    }
}

std::string ORCLogicalType::getTypeMappingSupported()
{
	return "\n           Type Mapping Supported\n"
		" External Data Type  |  Orc Data Type \n"
		" --------------------------------------------------------------------\n"
		" bool                |  boolean, bigint, int, smallint, tinyint, date\n"
		" int,int2,int4,int8  |  boolean, bigint, int, smallint, tinyint, date\n"
		" date                |  boolean, bigint, int, smallint, tinyint, date\n"
		" float4,float8       |  float,   double\n"
		" interval            |  string,  varchar\n"
		" time                |  string,  varchar\n"
		" timestamp           |  timestamp\n"
		" numeric             |  decimal\n"
		" char(n),varchar(n)  |  binary,  char, varchar, string\n"
		" bytea,text          |  binary,  char, varchar, string\n";
}



bool ParquetLogicalType::checkDataTypeCompatible(unsigned int typeOid, ::parquet::Type::type type)
{
	switch (typeOid)
    {
        case BOOLOID:
        case INT8OID:
        case INT4OID:
        case INT2OID:
        {
            if (type == ::parquet::Type::type::BOOLEAN ||
            type == ::parquet::Type::type::INT32 ||
            type == ::parquet::Type::type::INT64 ||
            type == ::parquet::Type::type::INT96)
            {
                return true;
            }
            break;
        }
        case FLOAT4OID:
        case FLOAT8OID:
        {
            if (type == ::parquet::Type::type::FLOAT ||
            type == ::parquet::Type::type::DOUBLE)
            {
                return true;
            }
            break;
        }
        case DATEOID:
        {
            if (type == ::parquet::Type::type::BOOLEAN ||
            type == ::parquet::Type::type::INT32 ||
            type == ::parquet::Type::type::INT64 ||
            type == ::parquet::Type::type::INT96)
            {
                return true;
            }
            break;
        }
        case INTERVALOID:
        case TIMEOID:
        {
            if (type == ::parquet::Type::type::BYTE_ARRAY ||
            type == ::parquet::Type::type::FIXED_LEN_BYTE_ARRAY)
            {
                return true;
            }
            break;
        }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
        {
            if (type == ::parquet::Type::type::INT32 ||
            type == ::parquet::Type::type::INT64 ||
            type == ::parquet::Type::type::INT96)
            {
                return true;
            }
            break;
        }
        case NUMERICOID:
        {
            if (type == ::parquet::Type::type::INT32 ||
            type == ::parquet::Type::type::INT64 ||
            type == ::parquet::Type::type::FIXED_LEN_BYTE_ARRAY ||
            type == ::parquet::Type::type::BYTE_ARRAY)
            {
                return true;
            }
            break;
        }
        case CHAROID:
        case BYTEAOID:
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
        {
            if (type == ::parquet::Type::type::BYTE_ARRAY ||
            type == ::parquet::Type::type::FIXED_LEN_BYTE_ARRAY)
            {
                return true;
            }
            break;
        }
        default:
            break;
    }
    return false;
}

std::string ParquetLogicalType::getTypeMappingSupported()
{
	return "\n           Type Mapping Supported\n"
		" External Data Type  |  Parquet Data Type \n"
		" --------------------------------------------------------------------\n"
		" bool                |  boolean\n"
		" int,int2,int4,int8  |  int32, int64\n"
		" date                |  int32\n"
		" float4,float8       |  float,   double\n"
		" interval            |  byteArray\n"
		" time                |  byteArray\n"
		" timestamp           |  int96, int64, int32\n"
		" numeric             |  int96\n"
		" char(n),varchar(n)  |  byteArray,  fixlenbyteArray\n"
		" bytea,text          |  byteArray,  fixlenbyteArray\n";
}

bool AvroLogicalType::isType(const avro::NodePtr &node, avro::Type type, const avro::LogicalType &ltype)
{
    if (node->type() != type || node->logicalType().type() != ltype.type())
    {
        return false;
    }
    if (ltype.type() == avro::LogicalType::DECIMAL)
    {
        int precision = node->logicalType().precision();
        int scale = node->logicalType().scale();
		/*
			The numeric in pg has two scenarios: 
			1. scale = -1 when precision and scale are not explicitly specified,
			2. scale >= 0 otherwise.
			In 1 scenarios, it is necessary to ensure that the precision of Avro decimal is less than
			the maximum precision we can parse (76 digits);
			In 2 scenarios, it just need to ensure the left part of avro decimal (precision - scale) is 
			less than or equal to it of pg numeric.
		*/
        if ((ltype.precision() == AVRO_NUMERIC_DEFAULT_SIZE && AVRO_NUMERIC_MAX_SIZE < precision) ||
            (ltype.precision() <= AVRO_NUMERIC_MAX_SIZE && ltype.precision() - ltype.scale() < precision - scale))
        {
            elog(WARNING, "decimal(%d, %d) out of range", precision, scale);
            return false;
        }
    }
    return true;
}

bool AvroLogicalType::hasType(const avro::NodePtr &node, avro::Type type, const avro::LogicalType &ltype)
{
    for (uint8_t i=0; i < node->leaves(); ++i)
    {
        if (isType(node->leafAt(i), type, ltype))
        {
            return true;
        }
    }
    return false;
}

bool AvroLogicalType::checkDataTypeCompatible(const avro::NodePtr &node, avro::Type type, const avro::LogicalType &ltype)
{
    if (node->type() == avro::AVRO_UNION)
    {
        if (node->leaves() == 2 && hasType(node, type, ltype) && hasType(node, avro::AVRO_NULL, avro::LogicalType(avro::LogicalType::NONE)))
        {
            return true;
        }
        else if (node->leaves() == 1 && hasType(node, type, ltype))
        {
            return true;
        }
    } 
    else if (isType(node, type, ltype))
    {
        return true;
    }
    return false;
}

int8_t AvroLogicalType::getNullBranchIdx(const avro::NodePtr &node)
{
    if (node->type() != avro::AVRO_UNION || node->leaves() > 2)
    {
        return -1;
    }
    int8_t count = node->leaves();
    for (int8_t i = 0; i < count; ++i)
    {
        if (node->leafAt(i)->type() == avro::AVRO_NULL)
        {
            return i;
        }
    }
    return -1;
}


std::string AvroLogicalType::getTypeMappingSupported()
{
    return "\n                      Type Mapping Supported\n"
			"| External Data Type       | Avro Data Type | Avro Logical Type \n"
			"| ------------------------ | -------------- | ------------------------- \n"
			"| bool                     | boolean        | none\n"
			"| int,int2,int 4           | int            | none\n"
			"| int 8                    | long           | none\n"
			"| float4                   | float          | none\n"
			"| float8                   | double         | none\n"
			"| bytea                    | bytes          | none\n"
			"| char(n),varchar(n),text  | string         | none\n"
			"| date                     | int            | date\n"
			"| time                     | int/long       | time (milli/micro)\n"
			"| timestamp                | long           | timestamp (milli/micro)\n"
			"| numeric                  | bytes          | decimal\n"
			"| UUID                     | string         | UUID\n";
}

}
}