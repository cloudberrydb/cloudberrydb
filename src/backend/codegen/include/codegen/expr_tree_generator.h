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
#ifndef GPCODEGEN_EXPR_TREE_GENERATOR_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_EXPR_TREE_GENERATOR_H_

#include <string>
#include <vector>
#include <memory>
#include <utility>
#include <unordered_map>

#include "codegen/utils/codegen_utils.h"

#include "llvm/IR/Value.h"

typedef struct ExprContext ExprContext;
typedef struct ExprState ExprState;
typedef struct Expr Expr;
typedef struct OpExpr OpExpr;
typedef struct Var Var;
typedef struct Const Const;

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

enum class ExprTreeNodeType {
  kConst = 0,
  kVar = 1,
  kOperator = 2
};

class ExprTreeGenerator {
 public:
  static bool VerifyAndCreateExprTree(
      ExprState* expr_state,
      ExprContext* econtext,
      std::unique_ptr<ExprTreeGenerator>& expr_tree);

  virtual bool GenerateCode(gpcodegen::CodegenUtils* codegen_utils,
                            ExprContext* econtext,
                            llvm::Function* llvm_main_func,
                            llvm::BasicBlock* llvm_error_block,
                            llvm::Value* llvm_isnull_arg,
                            llvm::Value* & value) = 0;

  ExprState* expr_state() { return expr_state_; }
protected:
  ExprTreeGenerator(ExprState* expr_state,
                    ExprTreeNodeType node_type) :
                      expr_state_(expr_state), node_type_(node_type) {}
 private:
  ExprState* expr_state_;
  ExprTreeNodeType node_type_;
  DISALLOW_COPY_AND_ASSIGN(ExprTreeGenerator);
};


/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_EXPR_TREE_GENERATOR_H_
