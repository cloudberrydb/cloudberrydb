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
#include <assert.h>
#include <stddef.h>
#include <cstdint>
#include <memory>
#include <string>

#include "codegen/base_codegen.h"
#include "codegen/codegen_wrapper.h"
#include "codegen/exec_eval_expr_codegen.h"
#include "codegen/expr_tree_generator.h"
#include "codegen/op_expr_tree_generator.h"
#include "codegen/slot_getattr_codegen.h"
#include "codegen/utils/gp_codegen_utils.h"
#include "codegen/utils/utility.h"

#include "llvm/IR/Argument.h"
#include "llvm/IR/Constant.h"
#include "llvm/IR/IRBuilder.h"


extern "C" {
#include "postgres.h"  // NOLINT(build/include)
#include "nodes/execnodes.h"
#include "utils/elog.h"
#include "executor/tuptable.h"
#include "nodes/nodes.h"
}

namespace llvm {
class BasicBlock;
class Function;
class Value;
}  // namespace llvm

using gpcodegen::ExecEvalExprCodegen;
using gpcodegen::SlotGetAttrCodegen;

constexpr char ExecEvalExprCodegen::kExecEvalExprPrefix[];

ExecEvalExprCodegen::ExecEvalExprCodegen
(
    ExecEvalExprFn regular_func_ptr,
    ExecEvalExprFn* ptr_to_regular_func_ptr,
    ExprState *exprstate,
    ExprContext *econtext,
    PlanState* plan_state) :
    BaseCodegen(kExecEvalExprPrefix,
                regular_func_ptr, ptr_to_regular_func_ptr),
                exprstate_(exprstate),
                econtext_(econtext),
                plan_state_(plan_state) {
}

void ExecEvalExprCodegen::PrepareSlotGetAttr(
    gpcodegen::GpCodegenUtils* codegen_utils, ExprTreeGeneratorInfo* gen_info) {
  assert(nullptr != plan_state_);
  switch (nodeTag(plan_state_)) {
    case T_SeqScanState:
    case T_TableScanState:
      // Generate dependent slot_getattr() implementation for the given slot
      if (gen_info->max_attr > 0) {
        TupleTableSlot* slot = reinterpret_cast<ScanState*>(plan_state_)
            ->ss_ScanTupleSlot;
        assert(nullptr != slot);
        std::string slot_getattr_func_name = "slot_getattr_"
            + std::to_string(reinterpret_cast<uint64_t>(slot)) + "_"
            + std::to_string(gen_info->max_attr);
        bool ok = SlotGetAttrCodegen::GenerateSlotGetAttr(
            codegen_utils, slot_getattr_func_name, slot, gen_info->max_attr,
            &gen_info->llvm_slot_getattr_func);
        if (!ok) {
          elog(DEBUG1, "slot_getattr generation for ExecEvalExpr failed!");
        }
      }
      break;
    case T_AggState:
      // For now, we assume that tuples for the Aggs are already going to be
      // deformed in which case, we can avoid generating and calling the
      // generated slot_getattr(). This may not be true always, but calling the
      // regular slot_getattr() will still preserve correctness.
      gen_info->llvm_slot_getattr_func = nullptr;
      break;
    default:
      elog(DEBUG1,
          "Attempting to generate ExecEvalExpr for an unsupported operator!");
  }
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

  // Prepare dependent slot_getattr() generation
  PrepareSlotGetAttr(codegen_utils, &gen_info);

  // In case the generation above failed either or it was not needed,
  // we revert to use the external slot_getattr()
  if (nullptr == gen_info.llvm_slot_getattr_func) {
    gen_info.llvm_slot_getattr_func =
        codegen_utils->GetOrRegisterExternalFunction(slot_getattr,
                                                     "slot_getattr");
  }

  irb->SetInsertPoint(llvm_entry_block);

#ifdef CODEGEN_DEBUG
  codegen_utils->CreateElog(
      DEBUG1,
      "Codegen'ed expression evaluation called!");
#endif


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

  llvm::Value* llvm_ret_value = codegen_utils->CreateCppTypeToDatumCast(value);
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
