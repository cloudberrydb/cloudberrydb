//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    pg_date_func_generator.h
//
//  @doc:
//    Base class for date functions to generate code
//
//---------------------------------------------------------------------------
#ifndef GPCODEGEN_PG_DATE_FUNC_GENERATOR_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_PG_DATE_FUNC_GENERATOR_H_

#include <memory>
#include <string>
#include <vector>

#include "codegen/base_codegen.h"
#include "codegen/pg_func_generator_interface.h"
#include "codegen/utils/codegen_utils.h"

#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Value.h"

namespace llvm {
class Value;
}  // namespace llvm

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

class GpCodegenUtils;
struct PGFuncGeneratorInfo;

/**
 * @brief Class with Static member function to generate code for date
 *        operators.
 **/

class PGDateFuncGenerator {
 public:
  /**
   * @brief Create instructions for date_le_timestamp function
   *
   * @param codegen_utils     Utility to easy code generation.
   * @param llvm_main_func    Current function for which we are generating code
   * @param llvm_error_block  Basic Block to jump when error happens
   * @param llvm_args         Vector of llvm arguments for the function
   * @param llvm_out_value    Store the results of function
   *
   * @return true if generation was successful otherwise return false
   *
   **/
  static bool DateLETimestamp(
      gpcodegen::GpCodegenUtils* codegen_utils,
      const PGFuncGeneratorInfo& pg_func_info,
      llvm::Value** llvm_out_value);

 private:
  /**
   * @brief Internal routines for promoting date to timestamp and timestamp
   *        with time zone (see date2timestamp).
   *
   * @param codegen_utils     Utility to easy code generation.
   * @param llvm_main_func    Current function for which we are generating code
   * @param llvm_arg          llvm value for the date
   * @param llvm_error_block  Basic Block to jump when error happens
   *
   * @note  If there is overflow, it will do elog::ERROR and then jump to given
   *        error block.
   */
  static llvm::Value* GenerateDate2Timestamp(
      GpCodegenUtils* codegen_utils,
      const PGFuncGeneratorInfo& pg_func_info);
};

/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_PG_ARITH_FUNC_GENERATOR_H_
