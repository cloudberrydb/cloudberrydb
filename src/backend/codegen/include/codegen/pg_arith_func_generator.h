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
#ifndef GPCODEGEN_PG_ARITH_FUNC_GENERATOR_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_PG_ARITH_FUNC_GENERATOR_H_

#include <string>
#include <vector>
#include <memory>

#include "codegen/utils/codegen_utils.h"

#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Value.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */
template <typename rtype, typename Arg0, typename Arg1>
class PGArithFuncGenerator {
 public:
  static bool MulWithOverflow(gpcodegen::CodegenUtils* codegen_utils,
                               llvm::Function* llvm_main_func,
                               llvm::BasicBlock* llvm_error_block,
                               std::vector<llvm::Value*>& llvm_args,
                               llvm::Value* & llvm_out_value);

  static bool AddWithOverflow(gpcodegen::CodegenUtils* codegen_utils,
                              llvm::Function* llvm_main_func,
                              llvm::BasicBlock* llvm_error_block,
                              std::vector<llvm::Value*>& llvm_args,
                              llvm::Value* & llvm_out_value);

  static bool SubWithOverflow(gpcodegen::CodegenUtils* codegen_utils,
                                llvm::Function* llvm_main_func,
                                llvm::BasicBlock* llvm_error_block,
                                std::vector<llvm::Value*>& llvm_args,
                                llvm::Value* & llvm_out_value);
};

template <typename rtype, typename Arg0, typename Arg1>
bool PGArithFuncGenerator<rtype, Arg0, Arg1>::MulWithOverflow(gpcodegen::CodegenUtils* codegen_utils,
                                          llvm::Function* llvm_main_func,
                                          llvm::BasicBlock* llvm_error_block,
                                          std::vector<llvm::Value*>& llvm_args,
                                          llvm::Value* & llvm_out_value) {
  // Assumed caller checked vector size and nullptr for codegen_utils
  llvm::BasicBlock* llvm_non_overflow_block = codegen_utils->CreateBasicBlock(
             "mul_non_overflow_block", llvm_main_func);
  llvm::BasicBlock* llvm_overflow_block = codegen_utils->CreateBasicBlock(
             "mul_overflow_block", llvm_main_func);

  llvm::Value* llvm_mul_output = codegen_utils->CreateMulOverflow<rtype>(
      llvm_args[0], llvm_args[1]);

  llvm::IRBuilder<>* irb = codegen_utils->ir_builder();

  llvm_out_value = irb->CreateExtractValue(llvm_mul_output, 0);
  llvm::Value* llvm_overflow_flag = irb->CreateExtractValue(llvm_mul_output, 1);

  irb->CreateCondBr(
      irb->CreateICmpEQ(llvm_overflow_flag,
                        codegen_utils->GetConstant<bool>(true)),
                        llvm_overflow_block,
                        llvm_non_overflow_block );

  irb->SetInsertPoint(llvm_overflow_block);
  // TODO : krajaraman Elog::ERROR after ElogWrapper integrad.
  irb->CreateBr(llvm_error_block);

  irb->SetInsertPoint(llvm_non_overflow_block);

  return true;
}

template <typename rtype, typename Arg0, typename Arg1>
bool PGArithFuncGenerator<rtype, Arg0, Arg1>::AddWithOverflow(
    gpcodegen::CodegenUtils* codegen_utils,
    llvm::Function* llvm_main_func,
    llvm::BasicBlock* llvm_error_block,
    std::vector<llvm::Value*>& llvm_args,
    llvm::Value* & llvm_out_value) {

  // Assumed caller checked vector size and nullptr for codegen_utils
  llvm::BasicBlock* llvm_non_overflow_block = codegen_utils->CreateBasicBlock(
      "add_non_overflow_block", llvm_main_func);
  llvm::BasicBlock* llvm_overflow_block = codegen_utils->CreateBasicBlock(
      "add_overflow_block", llvm_main_func);

  llvm::Value* llvm_mul_output = codegen_utils->CreateAddOverflow<rtype>(
      llvm_args[0], llvm_args[1]);

  llvm::IRBuilder<>* irb = codegen_utils->ir_builder();

  llvm_out_value = irb->CreateExtractValue(llvm_mul_output, 0);
  llvm::Value* llvm_overflow_flag = irb->CreateExtractValue(llvm_mul_output, 1);

  irb->CreateCondBr(
      irb->CreateICmpEQ(llvm_overflow_flag,
                        codegen_utils->GetConstant<bool>(true)),
                        llvm_overflow_block,
                        llvm_non_overflow_block );

  irb->SetInsertPoint(llvm_overflow_block);
  // TODO : krajaraman Elog::ERROR after ElogWrapper integrad.
  irb->CreateBr(llvm_error_block);

  irb->SetInsertPoint(llvm_non_overflow_block);

  return true;
}

template <typename rtype, typename Arg0, typename Arg1>
bool PGArithFuncGenerator<rtype, Arg0, Arg1>::SubWithOverflow(
    gpcodegen::CodegenUtils* codegen_utils,
    llvm::Function* llvm_main_func,
    llvm::BasicBlock* llvm_error_block,
    std::vector<llvm::Value*>& llvm_args,
    llvm::Value* & llvm_out_value) {

  // Assumed caller checked vector size and nullptr for codegen_utils
  llvm::BasicBlock* llvm_non_overflow_block = codegen_utils->CreateBasicBlock(
      "sub_non_overflow_block", llvm_main_func);
  llvm::BasicBlock* llvm_overflow_block = codegen_utils->CreateBasicBlock(
      "sub_overflow_block", llvm_main_func);

  llvm::Value* llvm_mul_output = codegen_utils->CreateSubOverflow<rtype>(
      llvm_args[0], llvm_args[1]);

  llvm::IRBuilder<>* irb = codegen_utils->ir_builder();

  llvm_out_value = irb->CreateExtractValue(llvm_mul_output, 0);
  llvm::Value* llvm_overflow_flag = irb->CreateExtractValue(llvm_mul_output, 1);

  irb->CreateCondBr(
      irb->CreateICmpEQ(llvm_overflow_flag,
                        codegen_utils->GetConstant<bool>(true)),
                        llvm_overflow_block,
                        llvm_non_overflow_block );

  irb->SetInsertPoint(llvm_overflow_block);
  // TODO : krajaraman Elog::ERROR after ElogWrapper integrad.
  irb->CreateBr(llvm_error_block);

  irb->SetInsertPoint(llvm_non_overflow_block);

  return true;
}

/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_PG_ARITH_FUNC_GENERATOR_H_
