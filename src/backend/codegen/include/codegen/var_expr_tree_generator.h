//---------------------------------------------------------------------------
//  Greenplum Database
//  Copyright (C) 2016 Pivotal Software, Inc.
//
//  @filename:
//    var_expr_tree_generator.h
//
//  @doc:
//    Object that generator code for variable expression.
//
//---------------------------------------------------------------------------
#ifndef GPCODEGEN_VAR_EXPR_TREE_GENERATOR_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_VAR_EXPR_TREE_GENERATOR_H_

#include "codegen/expr_tree_generator.h"

#include "llvm/IR/Value.h"

namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

/**
 * @brief Object that generator code for variable expression.
 **/
class VarExprTreeGenerator : public ExprTreeGenerator {
 public:
  static bool VerifyAndCreateExprTree(
        ExprState* expr_state,
        ExprContext* econtext,
        std::unique_ptr<ExprTreeGenerator>* expr_tree);

  bool GenerateCode(gpcodegen::CodegenUtils* codegen_utils,
                    ExprContext* econtext,
                    llvm::Function* llvm_main_func,
                    llvm::BasicBlock* llvm_error_block,
                    llvm::Value* llvm_isnull_arg,
                    llvm::Value** llvm_out_value) final;
 protected:
  /**
   * @brief Constructor.
   *
   * @param expr_state Expression state
   **/
  explicit VarExprTreeGenerator(ExprState* expr_state);
};

/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_VAR_EXPR_TREE_GENERATOR_H_
