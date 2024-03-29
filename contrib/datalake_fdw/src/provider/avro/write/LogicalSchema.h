#ifndef DATALAKE_AVRO_LOGICAL_SCHEMA_H
#define DATALAKE_AVRO_LOGICAL_SCHEMA_H

#include <avro/Schema.hh>
namespace avro {
class DateSchema : public Schema {
public:
    DateSchema() : Schema(new NodePrimitive(AVRO_INT)) {
        root()->setLogicalType(LogicalType(LogicalType::DATE));
    }
};
class TimeSchema : public Schema {
public:
    TimeSchema() : Schema(new NodePrimitive(AVRO_LONG)) {
        root()->setLogicalType(LogicalType(LogicalType::TIME_MICROS));
    }
};
class TimestampSchema : public Schema {
public:
    TimestampSchema() : Schema(new NodePrimitive(AVRO_LONG)) {
        root()->setLogicalType(LogicalType(LogicalType::TIMESTAMP_MICROS));
    }
};

class DecimalSchema : public Schema {
public:
    DecimalSchema(int precision, int scale) : Schema(new NodePrimitive(AVRO_BYTES)) {
        LogicalType lt(LogicalType::DECIMAL);
        lt.setPrecision(precision);
        lt.setScale(scale);
        root()->setLogicalType(lt);
    }
};

class DecimalFixSchema : public Schema {
public:
    DecimalFixSchema(int precision, int scale, int len) : Schema(new NodePrimitive(AVRO_FIXED)){
        LogicalType lt(LogicalType::DECIMAL);
        lt.setPrecision(precision);
        lt.setScale(scale);
        root()->setLogicalType(lt);
    }
};

class DurationSchema : public Schema {
public:
    DurationSchema() : Schema(new NodePrimitive(AVRO_LONG)) {
        root()->setLogicalType(LogicalType(LogicalType::DURATION));
    }
};

class UUIDSchema : public Schema {
public:
    UUIDSchema() : Schema(new NodePrimitive(AVRO_STRING)) {
        root()->setLogicalType(LogicalType(LogicalType::UUID));
    }
};

class OptionalSchema : public UnionSchema {
public:
    OptionalSchema(const Schema &schema) : UnionSchema() {
        addType(NullSchema());
        addType(schema);
    }
};

}
#endif //DATALAKE_AVRO_EXT_SCHEMA_H