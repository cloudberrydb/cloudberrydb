//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    base_codegen.h
//
//  @doc:
//    Base class for expression tree to generate code
//
//---------------------------------------------------------------------------
#ifndef GPCODEGEN_PG_FUNC_GENERATOR_INTERFACE_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_PG_FUNC_GENERATOR_INTERFACE_H_

#include <string>
#include <vector>
#include <memory>

#include "llvm/IR/Value.h"

#include "codegen/utils/codegen_utils.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

class PGFuncGeneratorInterface {
 public:
  virtual std::string GetName() = 0;
  virtual size_t GetTotalArgCount() = 0;
  virtual bool GenerateCode(gpcodegen::CodegenUtils* codegen_utils,
                            llvm::Function* llvm_main_func,
                            llvm::BasicBlock* llvm_error_block,
                            std::vector<llvm::Value*>& llvm_args,
                            llvm::Value* & llvm_out_value) = 0;
};

/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_PG_FUNC_GENERATOR_INTERFACE_H_
