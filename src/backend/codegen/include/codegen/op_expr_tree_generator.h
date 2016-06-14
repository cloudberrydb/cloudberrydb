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
#ifndef GPCODEGEN_OP_EXPR_TREE_GENERATOR_H_  // NOLINT(build/header_guard)
#define GPCODEGEN_OP_EXPR_TREE_GENERATOR_H_

#include "codegen/expr_tree_generator.h"
#include "codegen/pg_func_generator_interface.h"
#include "codegen/pg_func_generator.h"

#include "llvm/IR/Value.h"


namespace gpcodegen {

/** \addtogroup gpcodegen
 *  @{
 */

using CodeGenFuncMap = std::unordered_map<unsigned int,
    std::unique_ptr<gpcodegen::PGFuncGeneratorInterface>>;

class OpExprTreeGenerator : public ExprTreeGenerator {
 public:
  static void InitializeSupportedFunction();
  static bool VerifyAndCreateExprTree(
        ExprState* expr_state,
        ExprContext* econtext,
        std::unique_ptr<ExprTreeGenerator>& expr_tree);

  bool GenerateCode(gpcodegen::CodegenUtils* codegen_utils,
                    ExprContext* econtext,
                    llvm::Function* llvm_main_func,
                    llvm::BasicBlock* llvm_error_block,
                    llvm::Value* llvm_isnull_arg,
                    llvm::Value* & value) final;
 protected:
  OpExprTreeGenerator(ExprState* expr_state,
                      std::vector<std::unique_ptr<ExprTreeGenerator>>& arguments);
 private:
  std::vector<std::unique_ptr<ExprTreeGenerator>> arguments_;
  static CodeGenFuncMap supported_function_;
};


/** @} */
}  // namespace gpcodegen

#endif  // GPCODEGEN_OP_EXPR_TREE_GENERATOR_H_
