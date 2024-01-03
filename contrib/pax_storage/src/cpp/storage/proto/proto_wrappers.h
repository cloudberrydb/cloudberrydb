#pragma once

// The libproto defined `FATAL` inside as a marco linker
#undef FATAL
#include "storage/proto/micro_partition_stats.pb.h"
#include "storage/proto/orc_proto.pb.h"
#include "storage/proto/pax.pb.h"
#define FATAL 22
