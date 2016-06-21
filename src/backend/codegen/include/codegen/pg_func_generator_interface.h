//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    pg_func_generator_interface.h
//
//  @doc:
//    Interface for all Greenplum Function generator.
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

/**
 * @brief Interface for all code generators.
 **/
class PGFuncGeneratorInterface {
 public:
  virtual ~PGFuncGeneratorInterface() = default;

  /**
   * @return Greenplum function name for which it generate code
   **/
  virtual std::string GetName() = 0;

  /**
   * @return Total number of arguments for the function.
   **/
  virtual size_t GetTotalArgCount() = 0;

  /**
   * @brief Generate the code for Greenplum function.
   *
   * @param codegen_utils     Utility to easy code generation.
   * @param llvm_main_func    Current function for which we are generating code
   * @param llvm_error_block  Basic Block to jump when error happens
   * @param llvm_args         Vector of llvm arguments for the function
   * @param llvm_out_value    Store the results of function
   *
   * @return true when it generated successfully otherwise it return false.
   **/
  virtual bool GenerateCode(gpcodegen::CodegenUtils* codegen_utils,
                            llvm::Function* llvm_main_func,
                            llvm::BasicBlock* llvm_error_block,
                            const std::vector<llvm::Value*>& llvm_args,
                            llvm::Value** llvm_out_value) = 0;
};

/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_PG_FUNC_GENERATOR_INTERFACE_H_
