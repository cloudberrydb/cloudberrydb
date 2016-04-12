//---------------------------------------------------------------------------
// Greenplum Database
// Copyright (C) 2016 Pivotal Software, Inc.
//
// @filename:
//   init_codegen.cpp
//
// @doc:
//   C wrappers for initialization of codegen library.
//
//---------------------------------------------------------------------------

#include "codegen/init_codegen.h"

#include "codegen/utils/codegen_utils.h"

extern "C" int InitCodegen() {
  return gpcodegen::CodegenUtils::InitializeGlobal() ? 0 : 1;
}
