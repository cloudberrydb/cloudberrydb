//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    exec_eval_expr_codegen.cc
//
//  @doc:
//    Generates code for ExecEvalExpr function.
//
//---------------------------------------------------------------------------
#include <algorithm>
#include <cstdint>
#include <string>

#include "codegen/slot_getattr_codegen.h"
#include "codegen/exec_eval_expr_codegen.h"
#include "codegen/expr_tree_generator.h"
#include "codegen/op_expr_tree_generator.h"
#include "codegen/utils/clang_compiler.h"
#include "codegen/utils/utility.h"
#include "codegen/utils/codegen_utils.h"

#include "llvm/ADT/APFloat.h"
#include "llvm/ADT/APInt.h"
#include "llvm/IR/Argument.h"
#include "llvm/IR/BasicBlock.h"
#include "llvm/IR/Constant.h"
#include "llvm/IR/DerivedTypes.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/GlobalVariable.h"
#include "llvm/IR/Instruction.h"
#include "llvm/IR/Instructions.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/Type.h"
#include "llvm/IR/Value.h"
#include "llvm/IR/Verifier.h"
#include "llvm/Support/Casting.h"

extern "C" {
#include "postgres.h"  // NOLINT(build/include)
#include "utils/elog.h"
#include "nodes/execnodes.h"
}

using gpcodegen::ExecEvalExprCodegen;
using gpcodegen::SlotGetAttrCodegen;

constexpr char ExecEvalExprCodegen::kExecEvalExprPrefix[];

ExecEvalExprCodegen::ExecEvalExprCodegen
(
    ExecEvalExprFn regular_func_ptr,
    ExecEvalExprFn* ptr_to_regular_func_ptr,
    ExprState *exprstate,
    ExprContext *econtext,
    TupleTableSlot* slot) :
    BaseCodegen(kExecEvalExprPrefix,
                regular_func_ptr, ptr_to_regular_func_ptr),
                exprstate_(exprstate),
                econtext_(econtext),
                slot_(slot) {
}


bool ExecEvalExprCodegen::GenerateExecEvalExpr(
    gpcodegen::GpCodegenUtils* codegen_utils) {

  assert(NULL != codegen_utils);
  if (nullptr == exprstate_ ||
      nullptr == exprstate_->expr ||
      nullptr == econtext_) {
    return false;
  }
  // TODO(krajaraman): move to better place
  OpExprTreeGenerator::InitializeSupportedFunction();

  llvm::Function* exec_eval_expr_func = CreateFunction<ExecEvalExprFn>(
      codegen_utils, GetUniqueFuncName());

  // Function arguments to ExecVariableList
  llvm::Value* llvm_isnull_arg = ArgumentByPosition(exec_eval_expr_func, 2);

  // BasicBlock of function entry.
  llvm::BasicBlock* llvm_entry_block = codegen_utils->CreateBasicBlock(
      "entry", exec_eval_expr_func);
  llvm::BasicBlock* llvm_error_block = codegen_utils->CreateBasicBlock(
        "error_block", exec_eval_expr_func);

  auto irb = codegen_utils->ir_builder();

  // Check if we can codegen. If so create ExprTreeGenerator
  ExprTreeGeneratorInfo gen_info(econtext_,
                                 exec_eval_expr_func,
                                 llvm_error_block,
                                 nullptr,
                                 0);

  std::unique_ptr<ExprTreeGenerator> expr_tree_generator(nullptr);
  bool can_generate = ExprTreeGenerator::VerifyAndCreateExprTree(
      exprstate_, &gen_info, &expr_tree_generator);
  if (!can_generate ||
      expr_tree_generator.get() == nullptr) {
    return false;
  }

  // Generate dependant slot_getattr() implementation for the given slot
  if (gen_info.max_attr > 0) {
    assert(nullptr != slot_);

    std::string slot_getattr_func_name = "slot_getattr_" +
        std::to_string(reinterpret_cast<uint64_t>(slot_)) + "_" +
        std::to_string(gen_info.max_attr);

    bool ok = SlotGetAttrCodegen::GenerateSlotGetAttr(
        codegen_utils,
        slot_getattr_func_name,
        slot_,
        gen_info.max_attr,
        &gen_info.llvm_slot_getattr_func);
    if (!ok) {
      elog(DEBUG1, "slot_getattr generation for ExecEvalExpr failed!");
    }
  }

  // In case the generation above failed either or it was not needed,
  // we revert to use the external slot_getattr()
  if (nullptr == gen_info.llvm_slot_getattr_func) {
    gen_info.llvm_slot_getattr_func =
        codegen_utils->GetOrRegisterExternalFunction(slot_getattr);
  }

  irb->SetInsertPoint(llvm_entry_block);

  codegen_utils->CreateElog(
      DEBUG1,
      "Calling codegen'ed expression evaluation");


  // Generate code from expression tree generator
  llvm::Value* value = nullptr;
  bool is_generated = expr_tree_generator->GenerateCode(codegen_utils,
                                                        gen_info,
                                                        llvm_isnull_arg,
                                                        &value);
  if (!is_generated ||
      nullptr == value) {
    return false;
  }

  llvm::Value* llvm_ret_value = codegen_utils->CreateCast<int64_t>(value);
  irb->CreateRet(llvm_ret_value);

  irb->SetInsertPoint(llvm_error_block);
  irb->CreateRet(codegen_utils->GetConstant<int64_t>(0));
  return true;
}


bool ExecEvalExprCodegen::GenerateCodeInternal(GpCodegenUtils* codegen_utils) {
  bool isGenerated = GenerateExecEvalExpr(codegen_utils);

  if (isGenerated) {
    elog(DEBUG1, "ExecEvalExpr was generated successfully!");
    return true;
  } else {
    elog(DEBUG1, "ExecEvalExpr generation failed!");
    return false;
  }
}
