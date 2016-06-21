//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    const_expr_tree_generator.cc
//
//  @doc:
//    Object that generate code for const expression.
//
//---------------------------------------------------------------------------

#include "codegen/expr_tree_generator.h"
#include "codegen/const_expr_tree_generator.h"

#include "llvm/IR/Value.h"

extern "C" {
#include "postgres.h"  // NOLINT(build/include)
#include "nodes/execnodes.h"
}

using gpcodegen::ConstExprTreeGenerator;
using gpcodegen::ExprTreeGenerator;

bool ConstExprTreeGenerator::VerifyAndCreateExprTree(
    ExprState* expr_state,
    ExprContext* econtext,
    std::unique_ptr<ExprTreeGenerator>* expr_tree) {
  assert(nullptr != expr_state &&
         nullptr != expr_state->expr &&
         T_Const == nodeTag(expr_state->expr) &&
         nullptr != expr_tree);
  expr_tree->reset(new ConstExprTreeGenerator(expr_state));
  return true;
}

ConstExprTreeGenerator::ConstExprTreeGenerator(ExprState* expr_state) :
    ExprTreeGenerator(expr_state, ExprTreeNodeType::kConst) {
}

bool ConstExprTreeGenerator::GenerateCode(CodegenUtils* codegen_utils,
                                        ExprContext* econtext,
                                        llvm::Function* llvm_main_func,
                                        llvm::BasicBlock* llvm_error_block,
                                        llvm::Value* llvm_isnull_arg,
                                        llvm::Value** llvm_out_value) {
  assert(nullptr != llvm_out_value);
  Const* const_expr = reinterpret_cast<Const*>(expr_state()->expr);
  *llvm_out_value = codegen_utils->GetConstant(const_expr->constvalue);
  return true;
}
