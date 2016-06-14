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

#include "codegen/expr_tree_generator.h"
#include "codegen/const_expr_tree_generator.h"

#include "llvm/IR/Value.h"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "nodes/execnodes.h"
}

using gpcodegen::ConstExprTreeGenerator;
using gpcodegen::ExprTreeGenerator;

bool ConstExprTreeGenerator::VerifyAndCreateExprTree(
    ExprState* expr_state,
    ExprContext* econtext,
    std::unique_ptr<ExprTreeGenerator>& expr_tree) {
  assert(nullptr != expr_state && nullptr != expr_state->expr && T_Const == nodeTag(expr_state->expr));
  expr_tree.reset(new ConstExprTreeGenerator(expr_state));
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
                                        llvm::Value* & value) {
  Const* const_expr = (Const*)expr_state()->expr;
  value = codegen_utils->GetConstant(const_expr->constvalue);
  return true;
}
