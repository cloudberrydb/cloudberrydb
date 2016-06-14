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
#include <cassert>
#include "codegen/expr_tree_generator.h"
#include "codegen/op_expr_tree_generator.h"
#include "codegen/var_expr_tree_generator.h"
#include "codegen/const_expr_tree_generator.h"

#include "llvm/IR/Value.h"

extern "C" {
#include "postgres.h"
#include "utils/elog.h"
#include "nodes/execnodes.h"
}

using gpcodegen::ExprTreeGenerator;

bool ExprTreeGenerator::VerifyAndCreateExprTree(
    ExprState* expr_state,
    ExprContext* econtext,
    std::unique_ptr<ExprTreeGenerator>& expr_tree) {
  assert(nullptr != expr_state && nullptr != expr_state->expr);
  expr_tree.reset(nullptr);
  bool supported_expr_tree = false;
  switch(nodeTag(expr_state->expr)) {
    case T_OpExpr: {
       supported_expr_tree = OpExprTreeGenerator::VerifyAndCreateExprTree(expr_state,
                                                                         econtext,
                                                                         expr_tree);
       break;
    }
    case T_Var: {
      supported_expr_tree = VarExprTreeGenerator::VerifyAndCreateExprTree(expr_state,
                                                                          econtext,
                                                                          expr_tree);
      break;
    }
    case T_Const: {
      supported_expr_tree = ConstExprTreeGenerator::VerifyAndCreateExprTree(expr_state,
                                                                          econtext,
                                                                          expr_tree);
      break;
    }
    default : {
      supported_expr_tree = false;
      elog(DEBUG1, "Unsupported expression tree %d found", nodeTag(expr_state->expr));
    }
  }
  assert((!supported_expr_tree && nullptr == expr_tree.get()) ||
         (supported_expr_tree && nullptr != expr_tree.get()));
  return supported_expr_tree;
}
