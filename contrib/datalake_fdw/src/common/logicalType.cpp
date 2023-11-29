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

}
}