//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    pg_date_func_generator.cc
//
//  @doc:
//    Base class for date functions to generate code
//
//---------------------------------------------------------------------------


#include "codegen/pg_date_func_generator.h"
#include "codegen/utils/gp_codegen_utils.h"
#include "codegen/pg_arith_func_generator.h"

extern "C" {
#include "postgres.h"  // NOLINT(build/include)
#include "utils/elog.h"
#include "utils/date.h"
#include "utils/timestamp.h"
}

using gpcodegen::GpCodegenUtils;
using gpcodegen::PGDateFuncGenerator;

bool PGDateFuncGenerator::DateLETimestamp(
    gpcodegen::GpCodegenUtils* codegen_utils,
    llvm::Function* llvm_main_func,
    llvm::BasicBlock* llvm_error_block,
    const std::vector<llvm::Value*>& llvm_args,
    llvm::Value** llvm_out_value) {

  llvm::IRBuilder<>* irb = codegen_utils->ir_builder();

  // llvm_args[0] is of date type
  llvm::Value* llvm_arg0_Timestamp = GenerateDate2Timestamp(
      codegen_utils, llvm_main_func, llvm_args[0], llvm_error_block);

  // timestamp_cmp_internal {{{
#ifdef HAVE_INT64_TIMESTAMP
  *llvm_out_value =
      irb->CreateICmpSLE(llvm_arg0_Timestamp, llvm_args[1]);
#else
  // TODO(nikos): We do not support NaNs.
  elog(DEBUG1, "Timestamp != int_64: NaNs are not supported.");
  return false;
#endif
  // }}}

  return true;
}

llvm::Value* PGDateFuncGenerator::GenerateDate2Timestamp(
    GpCodegenUtils* codegen_utils,
    llvm::Function* llvm_main_func,
    llvm::Value* llvm_arg,
    llvm::BasicBlock* llvm_error_block) {

  assert(nullptr != llvm_arg && nullptr != llvm_arg->getType());

#ifdef HAVE_INT64_TIMESTAMP

  llvm::Value *llvm_USECS_PER_DAY = codegen_utils->
          GetConstant<int64_t>(USECS_PER_DAY);

  llvm::Value* llvm_out_value = nullptr;
  llvm::Value* llvm_err_msg = codegen_utils->GetConstant(
      "date out of range for timestamp");
  PGArithFuncGenerator<int64_t, int32_t, int64_t>::ArithOpWithOverflow(
      codegen_utils,
      &gpcodegen::GpCodegenUtils::CreateMulOverflow<int64_t>,
      llvm_main_func,
      llvm_error_block,
      llvm_err_msg,
      {llvm_arg, llvm_USECS_PER_DAY},
      &llvm_out_value);

  return llvm_out_value;
#else
  llvm::Value* llvm_arg_64 = codegen_utils->CreateCast<int64_t, int32_t>(
      llvm_arg);
  llvm::Value *llvm_SECS_PER_DAY = codegen_utils->
      GetConstant<int64_t>(SECS_PER_DAY);
  return codegen_utils->ir_builder()->CreateMul(llvm_arg_64, llvm_SECS_PER_DAY);
#endif
}
